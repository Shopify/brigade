// Package list walks an S3 bucket and saves all the keys in
// the bucket. The file will contain one key in JSON form per line, and
// will be compressed with gzip.
//
// Listing an S3 bucket is equivalent to walking a graph. Starting
// from the root node ("/"),
//    1. process the node (list all the S3 keys)
//    2. generate followers to the node (common prefixes from the List call)
//    3. visit each followers as in 1.
// While in a perfect world, the graph of an S3 bucket would be a tree,
// there are some duplicates and considering the bucket as a DAG is safer.
//
// So the algorithm is a BFS, where followers on the fringe are visited
// concurrently. The fringe contains Job objects, which are essentially
// holding the edge to visit + where to put the followers.
// When a worker visited a Job edge, it updates the Job and send it
// back on the `result` channel.
//
// The main loop keeps tracks of the jobs it has sent to workers (workSet),
// and of the followers it need to create Job objects for.
//
// After each iteration of the search, there are three possible states:
//  1: there were new followers, thus workset not empty.
//     loop will continue and wait for results.
//
//  2: there were no followers, but workset not yet empty.
//     loop will continue because further results are possible.
//
//  3: there were no followers, and workset is empty.
//     loop will stop because no further results are expected.
//
// If `dedup` is set, the search will track all visited nodes and avoid
// cycles. This will consume a lot more memory, but can avoid duplicate
// references to keys. Those duplicates are rare (<1000 over 40 millions).
// If we accept that duplicates occur, we can save a lot of memory by
// avoiding to track the set of visited edges (>40millions such edges).
package list

import (
	"encoding/json"
	"expvar"
	"fmt"
	"github.com/aybabtme/goamz/s3"
	"github.com/dustin/go-humanize"
	"io"
	"log"
	"math"
	"net/url"
	"sync"
	"time"
)

var (
	// MaxList is the maximum number of keys to accept from a call to LIST an s3
	// prefix.
	MaxList = 10000
	// Concurrency is the number of concurrent LIST request done on S3. At 200, the
	// worker queue is typically always busy and the network is saturated, so
	// there is no point increasing this.
	Concurrency = 200

	// MaxRetry is the number of time a worker will retry a LIST on a path,
	// given that the error was retryable.
	MaxRetry  = 5
	InitRetry = time.Second * 2
)

var (
	root = func(path string) *url.URL {
		u, err := url.Parse(path)
		if err != nil {
			log.Panicf("%q must be a valid URL: %v", path, err)
		}
		return u
	}("/")
)

type listTask struct {
	elog *log.Logger
}

var metrics = struct {
	workToDo        *expvar.Int
	followers       *expvar.Int
	totalKeys       *expvar.Int
	totalBucketSize *expvar.Int

	inflight         *expvar.Int
	secondsWaitingS3 *expvar.Float

	jobsAttempted *expvar.Int
	jobsOk        *expvar.Int
	jobsRescued   *expvar.Int
	jobsAbandoned *expvar.Int
}{
	workToDo:        expvar.NewInt("brigade.list.workToDo"),
	followers:       expvar.NewInt("brigade.list.followers"),
	totalKeys:       expvar.NewInt("brigade.list.totalKeys"),
	totalBucketSize: expvar.NewInt("brigade.list.totalBucketSize"),

	inflight:         expvar.NewInt("brigade.list.inflight"),
	secondsWaitingS3: expvar.NewFloat("brigade.list.secondsWaitingS3"),

	jobsAttempted: expvar.NewInt("brigade.list.jobsAttempted"),
	jobsOk:        expvar.NewInt("brigade.list.jobsOk"),
	jobsRescued:   expvar.NewInt("brigade.list.jobsRescued"),
	jobsAbandoned: expvar.NewInt("brigade.list.jobsAbandoned"),
}

// List an s3 bucket and write the keys in JSON form to dst. If dedup, will
// deduplicate all keys using a set (consumes more memory).
func List(el *log.Logger, sss *s3.S3, src string, dst io.Writer, dedup bool) error {

	srcU, srcErr := url.Parse(src)
	if srcErr != nil {
		return fmt.Errorf("not a valid bucket URL: %v", srcErr)
	}

	keys := make(chan s3.Key, Concurrency)

	// Start a key encoder, which writes to the file concurrently
	encDone := make(chan struct{})
	go func(w io.Writer) {
		log.Printf("start encoding keys to dst file")
		defer close(encDone)
		enc := json.NewEncoder(w)
		for k := range keys {
			if err := enc.Encode(k); err != nil {
				el.Fatalf("Couldn't encode key %q: %v", k.Key, err)
			}
		}
		log.Printf("done encoding keys to dst file")
	}(dst)

	// list all the keys in the source bucket, sending each key to the
	// file writer worker.
	log.Printf("starting the listing of all keys in %q, dedup=%v", srcU.String(), dedup)
	lister := listTask{elog: el}
	err := lister.listAllKeys(sss, srcU, dedup, func(k s3.Key) { keys <- k })
	// wait until the file writer is done
	log.Printf("done listing, waiting for key encoder to finish")
	close(keys)
	<-encDone
	if err != nil {
		return fmt.Errorf("couldn't list bucket: %v", err)
	}
	return nil
}

func (l *listTask) listAllKeys(sss *s3.S3, src *url.URL, dedup bool, f func(key s3.Key)) error {

	srcBkt := sss.Bucket(src.Host)

	var count uint64
	var size uint64
	start := time.Now()
	err := l.walkPath(srcBkt, src.Path, dedup, func(key s3.Key) {
		count++
		size += uint64(key.Size)
		f(key)
	})

	log.Printf("visited %d objects in %v, totalling %s", count, time.Since(start), humanize.Bytes(size))
	return err
}

func (l *listTask) walkPath(bkt *s3.Bucket, root string, dedup bool, keyVisitor func(key s3.Key)) error {

	// Data structures needed for the DF traversal
	fringe := make(chan *Job, Concurrency)
	result := make(chan *Job, Concurrency)

	followers := newLifoJob(metrics.followers)
	visited := make(map[string]struct{})
	workSet := newSet(metrics.workToDo)

	firstJob := newJob(root)

	workSet.Add(firstJob)
	fringe <- firstJob

	// Start the workers, which expands the edges of the fringe
	wg := sync.WaitGroup{}
	for i := 0; i < Concurrency; i++ {
		wg.Add(1)
		go l.listWorker(&wg, bkt, fringe, result)
	}

	for !workSet.IsEmpty() {

		var doneJob *Job
		if !followers.IsEmpty() {
			// if there are followers, either try to send
			// a new job on the fringe, or receive a result
			// from a worker
			select {
			case fringe <- followers.Peek():
				_ = followers.Remove()
				continue
			case doneJob = <-result:
			}
		} else {
			// if there are no followers, all we can do is
			// wait for a result to arrive
			doneJob = <-result
		}
		// remove the job from the set of jobs we are waiting for
		workSet.Delete(doneJob)

		// track some metrics

		metrics.jobsAttempted.Add(1)
		metrics.secondsWaitingS3.Add(doneJob.duration.Seconds())

		// if the job was in error, maybe try to reenqueue it
		if doneJob.err != nil {
			if doneJob.retryLeft > 0 {
				doneJob.retryLeft--
				l.elog.Printf("job=%d\tretrying\tretries=%d\terr=%v", doneJob.id, doneJob.retryLeft, doneJob.err)
				doneJob.err = nil
				followers.Add(doneJob)
				workSet.Add(doneJob)
			} else {
				metrics.jobsAbandoned.Add(1)
				l.elog.Printf("job=%d\tabandon\tretries=%d\terr=%v", doneJob.id, doneJob.retryLeft, doneJob.err)
			}
			continue
		} else if doneJob.retryLeft != MaxRetry {
			metrics.jobsRescued.Add(1)
			log.Printf("job=%d\trescued\tretries=%d", doneJob.id, doneJob.retryLeft)
		} else {
			metrics.jobsOk.Add(1)
		}

		l.visitKeys(doneJob.keys, keyVisitor, dedup, visited)

		for _, job := range l.jobsFromFollowers(doneJob.followers, workSet, dedup, visited) {
			followers.Add(job)
		}
	}

	log.Printf("no more jobs, closing")
	// invariant: no jobs are on the fringe, no results in the workset,
	// it is thus safe to tell the workers to stop waiting.
	close(fringe)
	// it's safe to close the results since no jobs are in, waiting for
	// results
	close(result)
	log.Printf("waiting for workers")
	wg.Wait()
	log.Printf("workers stopped")

	return nil
}

func (l *listTask) visitKeys(keys []s3.Key, visitor func(s3.Key), dedup bool, visited map[string]struct{}) {
	for _, key := range keys {
		if dedup {
			if _, ok := visited[key.Key]; ok {
				l.elog.Printf("deduplicate-key=%q", key.Key)
				continue
			}
			visited[key.Key] = struct{}{}
		}

		metrics.totalKeys.Add(1)
		metrics.totalBucketSize.Add(int64(key.Size))

		visitor(key)
	}
}

func (l *listTask) jobsFromFollowers(newFollowers []string, workset *jobSet, dedup bool, visited map[string]struct{}) []*Job {
	var newJobs []*Job
	for _, follow := range newFollowers {
		// prepare a new job, add it to the worker set and
		// send it to workers
		newjob := newJob(follow)

		if dedup {
			if _, ok := visited[newjob.path]; ok {
				l.elog.Printf("deduplicate-job=%q", newjob.path)
				continue
			}
		}

		// don't send jobs for paths we already planned to explore
		if workset.Contains(newjob) {
			// already queued
		} else {
			if dedup {
				visited[newjob.path] = struct{}{}
			}
			workset.Add(newjob)
			newJobs = append(newJobs, newjob)
		}
	}
	return newJobs
}

// list workers receives jobs and LIST the path in those jobs, sleeping between
// retryable errors before re-enqueing them.
func (l *listTask) listWorker(wg *sync.WaitGroup, bkt *s3.Bucket, jobs <-chan *Job, out chan<- *Job) {
	defer wg.Done()
	for job := range jobs {
		// track duration + inflight requests
		metrics.inflight.Add(1)
		start := time.Now()
		// list this path on the bkt
		res, err := bkt.List(job.path, "/", "", MaxList)
		job.duration = time.Since(start)
		metrics.inflight.Add(-1)

		if err != nil {
			job.err = err
			// when there's an error, sleep for a bit before reenqueuing
			// the job. This avoids retrying keys that are on a partition
			// that is overloaded, and various network issues.
			attemptsSoFar := float64(MaxRetry - job.retryLeft + 1)
			backoff := math.Pow(2.0, attemptsSoFar)
			sleepFor := time.Duration(backoff) * InitRetry
			l.elog.Printf("worker-sleep-on-error=%v\tbackoff=%v", sleepFor, backoff)
			time.Sleep(sleepFor)
			l.elog.Printf("worker-woke-up")
		} else {
			// if all went well, set the job results
			job.keys = res.Contents
			job.followers = res.CommonPrefixes
		}
		// send the job result back to the main loop, which will
		// decided to reenqueue or not if there was an error
		out <- job
	}
}

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
// The main loop keeps trakcs of the jobs it has sent to workers (workSet),
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
	"fmt"
	"github.com/aybabtme/goamz/s3"
	"github.com/dustin/go-humanize"
	"io"
	"log"
	"math"
	"net/url"
	"sync"
	"sync/atomic"
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
	elog *log.Logger
	root = func(path string) *url.URL {
		u, err := url.Parse(path)
		if err != nil {
			log.Panicf("%q must be a valid URL: %v", path, err)
		}
		return u
	}("/")
)

// List an s3 bucket and write the keys in JSON form to dst. If dedup, will
// deduplicate all keys using a set (consumes more memory).
func List(el *log.Logger, sss *s3.S3, src string, dst io.Writer, dedup bool) error {

	elog = el
	srcU, srcErr := url.Parse(src)
	if srcErr != nil {
		return fmt.Errorf("not a valid bucket URL: %v", srcErr)
	}

	keys := make(chan s3.Key, Concurrency)

	// Start a key encoder, which writes to the file concurrently
	encDone := make(chan struct{})
	go func(w io.Writer) {
		defer close(encDone)
		enc := json.NewEncoder(w)
		for k := range keys {
			if err := enc.Encode(k); err != nil {
				elog.Fatalf("Couldn't encode key %q: %v", k.Key, err)
			}
		}
	}(dst)

	// list all the keys in the source bucket, sending each key to the
	// file writer worker.
	err := listAllKeys(sss, srcU, dedup, func(k s3.Key) { keys <- k })
	// wait until the file writer is done
	close(keys)
	<-encDone
	if err != nil {
		return fmt.Errorf("couldn't list bucket: %v", err)
	}
	return nil
}

func listAllKeys(sss *s3.S3, src *url.URL, dedup bool, f func(key s3.Key)) error {

	srcBkt := sss.Bucket(src.Host)

	var count uint64
	var size uint64
	start := time.Now()
	err := walkPath(srcBkt, src.Path, dedup, func(key s3.Key) {
		count++
		size += uint64(key.Size)
		f(key)
	})

	log.Printf("visited %d objects in %v, totalling %s", count, time.Since(start), humanize.Bytes(size))
	return err
}

func walkPath(bkt *s3.Bucket, root string, dedup bool, keyVisitor func(key s3.Key)) error {

	// Data structures needed for the BFS
	fringe := make(chan *Job, Concurrency)
	result := make(chan *Job, Concurrency)

	// LIFO will do a DF traversal
	followers := &lifoJobs{}
	visited := make(map[string]struct{})
	workSet := make(jobSet)

	firstJob := newJob(root)

	workSet.Add(firstJob)
	fringe <- firstJob

	// Stats to track progress
	stats := newWalkStats()
	defer stats.Stop()

	// Start the workers, which expands the edges of the fringe
	wg := sync.WaitGroup{}
	for i := 0; i < Concurrency; i++ {
		wg.Add(1)
		go listWorker(&wg, bkt, fringe, result)
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
		stats.Lock()
		stats.worktodo = int64(len(workSet))
		stats.followers = int64(followers.Len())
		stats.jobspersec++
		stats.qtstream.Insert(float64(doneJob.duration.Nanoseconds()))
		stats.Unlock()

		// if the job was in error, maybe try to reenqueue it
		if doneJob.err != nil {
			if doneJob.retryLeft > 0 {
				doneJob.retryLeft--
				elog.Printf("job=%d\tretrying\tretries=%d\terr=%v", doneJob.id, doneJob.retryLeft, doneJob.err)
				doneJob.err = nil
				followers.Add(doneJob)
				workSet.Add(doneJob)
			} else {
				elog.Printf("job=%d\tabandon\tretries=%d\terr=%v", doneJob.id, doneJob.retryLeft, doneJob.err)
			}
			continue
		} else if doneJob.retryLeft != MaxRetry {
			log.Printf("job=%d\trescued\tretries=%d", doneJob.id, doneJob.retryLeft)
		}

		visitKeys(doneJob.keys, keyVisitor, dedup, visited, stats)

		for _, job := range jobsFromFollowers(doneJob.followers, workSet, dedup, visited) {
			followers.Add(job)
		}

		stats.Lock()
		stats.newkeys += int64(len(doneJob.keys))
		stats.newleads += int64(len(doneJob.followers))
		stats.Unlock()
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

func visitKeys(keys []s3.Key, visitor func(s3.Key), dedup bool, visited map[string]struct{}, stats *walkStats) {
	for _, key := range keys {
		if dedup {
			if _, ok := visited[key.Key]; ok {
				elog.Printf("deduplicate-key=%q", key.Key)
				continue
			}
			visited[key.Key] = struct{}{}
		}

		stats.totalkeys++
		stats.totalsize += uint64(key.Size)
		visitor(key)
	}
}

func jobsFromFollowers(newFollowers []string, workset jobSet, dedup bool, visited map[string]struct{}) []*Job {
	var newJobs []*Job
	for _, follow := range newFollowers {
		// prepare a new job, add it to the worker set and
		// send it to workers
		newjob := newJob(follow)

		if dedup {
			if _, ok := visited[newjob.path]; ok {
				elog.Printf("deduplicate-job=%q", newjob.path)
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

// inflight tracks how many requests to S3 are currently inflight. Use atomic calls
// to access this value.
var inflight int64

// list workers receives jobs and LIST the path in those jobs, sleeping between
// retryable errors before re-enqueing them.
func listWorker(wg *sync.WaitGroup, bkt *s3.Bucket, jobs <-chan *Job, out chan<- *Job) {
	defer wg.Done()
	for job := range jobs {
		// track duration + inflight requests
		atomic.AddInt64(&inflight, 1)
		start := time.Now()
		// list this path on the bkt
		res, err := bkt.List(job.path, "/", "", MaxList)
		job.duration = time.Since(start)
		atomic.AddInt64(&inflight, -1)

		if err != nil {
			job.err = err
			// when there's an error, sleep for a bit before reenqueuing
			// the job. This avoids retrying keys that are on a partition
			// that is overloaded, and various network issues.
			attempsSoFar := float64(MaxRetry - job.retryLeft + 1)
			backoff := math.Pow(2.0, attempsSoFar)
			sleepFor := time.Duration(backoff) * InitRetry
			elog.Printf("worker-sleep-on-error=%v\tbackoff=%v", sleepFor, backoff)
			time.Sleep(sleepFor)
			elog.Printf("worker-woke-up")
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

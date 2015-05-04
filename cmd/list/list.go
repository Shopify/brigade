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
package list

import (
	"encoding/json"
	"expvar"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/pushrax/goamz/s3"
	"io"
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
	Concurrency = 1000

	// MaxRetry is the number of time a worker will retry a LIST on a path,
	// given that the error was retryable.
	MaxRetry  = 5
	InitRetry = time.Second * 2
)

var (
	root = func(path string) *url.URL {
		u, err := url.Parse(path)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"path":  path,
				"error": err,
			}).Panic("invalid URL")

		}
		return u
	}("/")
)

type listTask struct{}

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

// List an s3 bucket and write the keys in JSON form to dst.
func List(sss *s3.S3, bucket, prefix string, dst io.Writer) error {
	keys := make(chan s3.Key, Concurrency)

	// Start a key encoder, which writes to the file concurrently
	encDone := make(chan struct{})
	go func(w io.Writer) {
		logrus.Info("start encoding keys to destination writer")
		defer close(encDone)
		enc := json.NewEncoder(w)
		for k := range keys {
			if err := enc.Encode(k); err != nil {
				logrus.WithFields(logrus.Fields{
					"key":   k,
					"error": err,
				}).Fatal("couldn't encode key to destination")
				return
			}
		}
		logrus.Info("done encoding keys to dst file")
	}(dst)

	// list all the keys in the source bucket, sending each key to the
	// file writer worker.

	logrus.WithField("bucket_source", bucket).Info("starting the listing of all keys in bucket")
	lister := listTask{}
	err := lister.listAllKeys(sss, bucket, prefix, func(k s3.Key) { keys <- k })
	// wait until the file writer is done
	logrus.Info("done listing, waiting for key encoder to finish")
	close(keys)
	<-encDone
	if err != nil {
		return fmt.Errorf("couldn't list bucket: %v", err)
	}
	return nil
}

func (l *listTask) listAllKeys(sss *s3.S3, bucket, prefix string, f func(key s3.Key)) error {

	srcBkt := sss.Bucket(bucket)

	var count uint64
	var size uint64
	start := time.Now()
	err := l.walkPath(srcBkt, prefix, func(key s3.Key) {
		count++
		size += uint64(key.Size)
		f(key)
	})

	logrus.WithFields(logrus.Fields{
		"bucket_source":       bucket,
		"bucket_object_count": count,
		"duration":            time.Since(start),
		"bucket_total_size":   size,
	}).Info("done visiting bucket")

	return err
}

func (l *listTask) walkPath(bkt *s3.Bucket, root string, keyVisitor func(key s3.Key)) error {

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

				logrus.WithFields(logrus.Fields{
					"error":   doneJob.err,
					"retries": doneJob.retryLeft,
					"job_id":  doneJob.id,
				}).Debug("retrying failed job")

				doneJob.err = nil
				followers.Add(doneJob)
				workSet.Add(doneJob)
			} else {
				metrics.jobsAbandoned.Add(1)

				logrus.WithFields(logrus.Fields{
					"key":     doneJob.path,
					"error":   doneJob.err,
					"retries": doneJob.retryLeft,
					"job_id":  doneJob.id,
				}).Error("abandoning failed job after too many retries")

			}
			continue
		} else if doneJob.retryLeft != MaxRetry {
			metrics.jobsRescued.Add(1)
			logrus.WithFields(logrus.Fields{
				"job":     doneJob.id,
				"retries": doneJob.retryLeft,
			}).Debug("job rescued from error")
		} else {
			metrics.jobsOk.Add(1)
		}

		l.visitKeys(doneJob.keys, keyVisitor, visited)

		for _, job := range l.jobsFromFollowers(doneJob.followers, workSet, visited) {
			followers.Add(job)
		}
	}

	logrus.Info("no more jobs, closing")
	// invariant: no jobs are on the fringe, no results in the workset,
	// it is thus safe to tell the workers to stop waiting.
	close(fringe)
	// it's safe to close the results since no jobs are in, waiting for
	// results
	close(result)
	logrus.Info("waiting for workers")
	wg.Wait()
	logrus.Info("workers stopped")

	return nil
}

func (l *listTask) visitKeys(keys []s3.Key, visitor func(s3.Key), visited map[string]struct{}) {
	for _, key := range keys {
		if _, ok := visited[key.Key]; ok {
			logrus.WithField("key", key).Warn("deduplicate key")
			continue
		}
		visited[key.Key] = struct{}{}
		metrics.totalKeys.Add(1)
		metrics.totalBucketSize.Add(int64(key.Size))

		visitor(key)
	}
}

func (l *listTask) jobsFromFollowers(newFollowers []string, workset *jobSet, visited map[string]struct{}) []*Job {
	var newJobs []*Job
	for _, follow := range newFollowers {
		// prepare a new job, add it to the worker set and
		// send it to workers
		newjob := newJob(follow)

		if _, ok := visited[newjob.path]; ok {
			logrus.WithField("path", newjob.path).Warn("duplicate job path")
			continue
		}

		// don't send jobs for paths we already planned to explore
		if workset.Contains(newjob) {
			// already queued
		} else {
			visited[newjob.path] = struct{}{}
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
		// track duration
		start := time.Now()
		marker := ""

		for {
			metrics.inflight.Add(1)

			// list this path on the bkt
			res, err := bkt.List(job.path, "/", marker, MaxList)
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

				logrus.WithField("backoff", backoff).Debug("worker is sleeping on error")
				time.Sleep(sleepFor)
				logrus.WithField("backoff", backoff).Debug("worker woke up")
			} else {
				// if all went well, set the job results
				job.keys = append(job.keys, res.Contents...)
				job.followers = append(job.followers, res.CommonPrefixes...)
			}

			if res == nil || !res.IsTruncated {
				break
			}

			marker = res.Contents[len(res.Contents)-1].Key
		}

		// send the job result back to the main loop, which will
		// decided to reenqueue or not if there was an error
		out <- job
	}
}

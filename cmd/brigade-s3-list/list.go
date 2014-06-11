// Command brigade-s3-sync walks an S3 bucket and saves all the keys in
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
package main

import (
	"compress/gzip"
	"encoding/json"
	"flag"
	"github.com/aybabtme/color/brush"
	"github.com/bmizerany/perks/quantile"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"github.com/davecheney/profile"
	"github.com/dustin/go-humanize"
	"io"
	"log"
	"net/url"
	"os"
	"path"
	"runtime"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"
)

const (
	// MaxList is the maximum number of keys to accept from a call to LIST an s3
	// prefix.
	MaxList = 10000
	// Concurrency is the number of concurrent LIST request done on S3. At 200, the
	// worker queue is typically always busy and the network is saturated, so
	// there is no point increasing this.
	Concurrency = 200

	// MaxRetry is the number of time a worker will retry a LIST on a path,
	// given that the error was retryable.
	MaxRetry = 5
)

var (
	q    = struct{}{}
	root = mustURL("/")
	elog *log.Logger
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	w := tabwriter.NewWriter(os.Stdout, 16, 2, 2, ' ', 0)
	log.SetOutput(&lineTabWriter{w})
	log.SetFlags(log.Ltime)
	log.SetPrefix(brush.Blue("[info] ").String())
}

// Job holds the state of visiting an path in S3. This state includes
// the last error, the number of retries left, the duration of the
// last call, they keys found in the path and the followers at that path.
type Job struct {
	id        uint64
	path      string
	duration  time.Duration
	retryLeft int
	err       error
	keys      []s3.Key
	followers []string
}

var jobIDCounter uint64

func newJob(rel string) *Job {
	var relpath string
	if path.IsAbs(rel) {
		relpath = rel[1:]
	} else {
		relpath = rel
	}
	return &Job{
		id:        atomic.AddUint64(&jobIDCounter, 1),
		retryLeft: MaxRetry,
		path:      relpath,
		// other fields nil-value is correct
	}
}

func main() {

	// setup the error log to write on stderr and to a file
	efile, err := os.OpenFile(os.Args[0]+".elog", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0640)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { lognotnil(efile.Close()) }()
	elog = log.New(io.MultiWriter(os.Stderr, efile), brush.Red("[error] ").String(), log.Ltime|log.Lshortfile)

	// read the flags
	var (
		access      = os.Getenv("AWS_ACCESS_KEY")
		secret      = os.Getenv("AWS_SECRET_KEY")
		src         = flag.String("bkt", "", "path to bucket, of the form s3://name/path/")
		dst         = flag.String("dst", "bucket_list.json.gz", "filename to which the list of keys is saved")
		memprof     = flag.Bool("memprof", false, "track a memory profile")
		deduplicate = flag.Bool("deduplicate", false, "deduplicate jobs and keys, consumes much more memory")
		regionName  = flag.String("aws-region", aws.USEast.Name, "AWS region")
	)
	flag.Parse()

	srcU, srcErr := url.Parse(*src)
	region, validRegion := aws.Regions[*regionName]
	switch {
	case access == "":
		flagFatal("needs an AWS access key\n")
	case secret == "":
		flagFatal("needs an AWS secret key\n")
	case *src == "":
		flagFatal("needs a source bucket\n")
	case srcErr != nil:
		flagFatal("%q is not a valid source URL: %v", *src, srcErr)
	case !validRegion:
		flagFatal("%q is not a valid region name", *regionName)
	}

	auth := aws.Auth{AccessKey: access, SecretKey: secret}
	sss := s3.New(auth, region)

	if *memprof {
		defer profile.Start(profile.MemProfile).Stop()
	}

	// create the output file for the bucket keys
	file, err := os.Create(*dst)
	if err != nil {
		elog.Fatalf("couldn't open %q: %v", *dst, err)
	}
	defer func() { lognotnil(file.Close()) }()
	gw := gzip.NewWriter(file)
	defer func() { lognotnil(gw.Close()) }()

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
	}(gw)

	// list all the keys in the source bucket, sending each key to the
	// file writer worker.
	err = listAllKeys(sss, srcU, *deduplicate, func(k s3.Key) { keys <- k })
	// wait until the file writer is done
	close(keys)
	<-encDone

	if err != nil {
		elog.Fatalf("Couldn't list buckets: %v", err)
	}
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

func walkPath(bkt *s3.Bucket, root string, dedup bool, f func(key s3.Key)) error {

	// Datastructures needed for the BFS
	fringe := make(chan *Job, Concurrency)
	result := make(chan *Job, Concurrency)

	visited := make(map[string]struct{})
	workSet := make(map[string]struct{})
	var followers []*Job

	firstJob := newJob(root)

	workSet[firstJob.path] = q
	fringe <- firstJob

	// Stats to track progress
	qtstream := quantile.NewTargeted(0.50, 0.95)
	var lastRep time.Time
	var newkeys, newleads, totalkeys, jobspersec int64
	var totalsize uint64
	var mem runtime.MemStats

	// Start the workers, which expand the edges of the fringe
	wg := sync.WaitGroup{}
	for i := 0; i < Concurrency; i++ {
		wg.Add(1)
		go listWorker(&wg, bkt, fringe, result)
	}

	for len(workSet) != 0 {

		// every second, print some stats about current state of the world.
		if time.Since(lastRep) > time.Second {
			runtime.ReadMemStats(&mem)
			p50 := time.Duration(int64(qtstream.Query(0.50)))
			p95 := time.Duration(int64(qtstream.Query(0.95)))
			qtstream.Reset()
			log.Printf("mem=%s\tjobs/s=%s\twork=%s\tfollow=%s\tinflight=%s\tkeys=%s\tbktsize=%s\tnew=%s\tleads=%s\tp50=%v\tp95=%v",
				humanize.Bytes(mem.Sys-mem.HeapReleased),
				humanize.Comma(jobspersec),
				humanize.Comma(int64(len(workSet))),
				humanize.Comma(int64(len(followers))),
				humanize.Comma(atomic.LoadInt64(&inflight)),
				humanize.Comma(totalkeys),
				humanize.Bytes(totalsize),
				humanize.Comma(newkeys),
				humanize.Comma(newleads),
				p50,
				p95)
			newkeys = 0
			newleads = 0
			jobspersec = 0
			lastRep = time.Now()
		}

		var doneJob *Job
		if len(followers) > 0 {
			// if there are followers, either try to send
			// a new job on the fringe, or receive a result
			// from a worker
			select {
			case fringe <- followers[len(followers)-1]:
				followers = followers[:len(followers)-1]
				continue
			case doneJob = <-result:
			}
		} else {
			// if there are no followers, all we can do is
			// wait for a result to arrive
			doneJob = <-result
		}
		// track some metrics
		jobspersec++
		qtstream.Insert(float64(doneJob.duration.Nanoseconds()))

		// remove the job from the set of jobs we are waiting for
		delete(workSet, doneJob.path)

		// if the job was in error, maybe try to reenqueue it
		if doneJob.err != nil {
			if doneJob.retryLeft > 0 {
				doneJob.retryLeft--
				doneJob.err = nil
				elog.Printf("job %d failed, %d retries left: %v",
					doneJob.id,
					doneJob.retryLeft,
					doneJob.err)
				followers = append(followers, doneJob)
			} else {
				elog.Printf("job %d failed, retries exhausted! %v",
					doneJob.id, doneJob.err)
			}
			continue
		}

		// invoke call back for each key
		for _, key := range doneJob.keys {
			if dedup {
				if _, ok := visited[key.Key]; ok {
					elog.Printf("deduplicate-key=%q", key.Key)
					continue
				}
				visited[key.Key] = q
			}

			totalkeys++
			totalsize += uint64(key.Size)
			f(key)
		}

		// for each follower
		for _, follow := range doneJob.followers {
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
			if _, ok := workSet[newjob.path]; ok {
				// already queued
			} else {
				if dedup {
					visited[newjob.path] = q
				}
				workSet[newjob.path] = q
				followers = append(followers, newjob)
			}
		}
		newkeys += int64(len(doneJob.keys))
		newleads += int64(len(doneJob.followers))
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
			attempsSoFar := MaxRetry - job.retryLeft + 1
			sleepFor := time.Second * time.Duration(attempsSoFar)
			elog.Printf("worker-sleep-on-error=%v", sleepFor)
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

// Helpers

// Helper for URL creation.
func mustURL(path string) *url.URL {
	u, err := url.Parse(path)
	if err != nil {
		log.Fatalf("%q must be a valid URL: %v", path, err)
	}
	return u
}

// flagFatal is the same as elog.Fatalf, but prints the flag usages before
// exiting the process.
func flagFatal(format string, args ...interface{}) {
	elog.Printf(format, args...)
	flag.PrintDefaults()
	os.Exit(2)
}

func lognotnil(err error) {
	if err != nil {
		elog.Print(err)
	}
}

// Pretty print, aligned logs.
type lineTabWriter struct {
	tab *tabwriter.Writer
}

func (l *lineTabWriter) Write(p []byte) (int, error) {
	n, err := l.tab.Write(p)
	if err != nil {
		return n, err
	}
	return n, l.tab.Flush()
}

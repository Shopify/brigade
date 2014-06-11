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
	MaxList = 10000
	Para    = 200
)

var (
	q    = struct{}{}
	elog *log.Logger
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	w := tabwriter.NewWriter(os.Stdout, 16, 2, 2, ' ', 0)
	log.SetOutput(&lineTabWriter{w})
	log.SetFlags(log.Ltime)
	log.SetPrefix(brush.Blue("[info] ").String())

}

func pfatal(format string, args ...interface{}) {
	elog.Printf(format, args...)
	flag.PrintDefaults()
	os.Exit(2)
}

func mustURL(path string) *url.URL {
	u, err := url.Parse(path)
	if err != nil {
		log.Fatalf("%q must be a valid URL: %v", path, err)
	}
	return u
}

func main() {

	efile, err := os.OpenFile(os.Args[0]+".elog", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0640)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = efile.Close() }()
	elog = log.New(io.MultiWriter(os.Stderr, efile),
		brush.Red("[error] ").String(),
		log.Ltime|log.Lshortfile)

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
		pfatal("needs an AWS access key\n")
	case secret == "":
		pfatal("needs an AWS secret key\n")
	case *src == "":
		pfatal("needs a source bucket\n")
	case srcErr != nil:
		pfatal("%q is not a valid source URL: %v", *src, srcErr)
	case !validRegion:
		pfatal("%q is not a valid region name", *regionName)
	}

	auth := aws.Auth{AccessKey: access, SecretKey: secret}
	sss := s3.New(auth, region)

	if *memprof {
		defer profile.Start(profile.MemProfile).Stop()
	}

	file, err := os.Create(*dst)
	if err != nil {
		pfatal("couldn't open %q: %v", *dst, err)
	}
	defer func() { _ = file.Close() }()
	gw := gzip.NewWriter(file)
	defer func() { _ = gw.Close() }()

	keys := make(chan s3.Key, Para)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func(wg *sync.WaitGroup, w io.Writer) {
		defer wg.Done()
		enc := json.NewEncoder(w)
		for k := range keys {
			if err := enc.Encode(k); err != nil {
				log.Fatalf("Couldn't encode key %q: %v", k.Key, err)
			}
		}
	}(&wg, gw)

	err = listAllKeys(sss, srcU, *deduplicate, func(k s3.Key) { keys <- k })
	close(keys)
	wg.Wait()

	if err != nil {
		log.Fatalf("Couldn't list buckets: %v", err)
	}
}

var root = mustURL("/")

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

	fringe := make(chan *Job, Para)
	result := make(chan *Job, Para)

	wg := sync.WaitGroup{}
	for i := 0; i < Para; i++ {
		wg.Add(1)
		go listWorker(&wg, bkt, fringe, result)
	}

	visited := make(map[string]struct{})
	workSet := make(map[string]struct{})

	firstJob := newJob(root)

	workSet[firstJob.path] = q
	fringe <- firstJob

	qtstream := quantile.NewTargeted(0.50, 0.95)
	var lastRep time.Time
	var newkeys, newleads, totalkeys, jobspersec int64
	var totalsize uint64

	mem := runtime.MemStats{}

	// while there are jobs sent to workers
	var followers []*Job

	for len(workSet) != 0 {

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
			select {
			case fringe <- followers[len(followers)-1]:
				followers = followers[:len(followers)-1]
				continue
			case doneJob = <-result:
			}
		} else {
			doneJob = <-result
		}

		delete(workSet, doneJob.path)

		jobspersec++
		qtstream.Insert(float64(doneJob.duration.Nanoseconds()))
		if doneJob.err != nil {
			err := doneJob.err
			if doneJob.retryLeft > 0 {
				doneJob.retryLeft--
				doneJob.err = nil
				elog.Printf("job %d failed, %d retries left: %v",
					doneJob.id,
					doneJob.retryLeft,
					err)
				followers = append(followers, doneJob)
			} else {
				elog.Printf("job %d failed, retries exhausted! %v",
					doneJob.id, err)
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
		// three cases:
		//  1: there were new followers, thus workset not empty.
		//     loop will continue and wait for results.
		//
		//  2: there were no followers, but workset not yet empty.
		//     loop will continue because further results are possible.
		//
		//  3: there were no followers, and workset is empty.
		//     loop will stop because no further results are expected.
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

var inflight int64

func listWorker(wg *sync.WaitGroup, bkt *s3.Bucket, jobs <-chan *Job, out chan<- *Job) {
	defer wg.Done()
	for job := range jobs {
		atomic.AddInt64(&inflight, 1)
		start := time.Now()
		res, err := bkt.List(job.path, "/", "", MaxList)
		job.duration = time.Since(start)
		atomic.AddInt64(&inflight, -1)

		if err != nil {
			job.err = err
			sleepFor := time.Second * time.Duration(job.retryLeft)
			elog.Printf("worker-sleep-on-error=%v", sleepFor)
			time.Sleep(sleepFor)
			elog.Printf("worker-woke-up")
		} else {
			job.keys = res.Contents
			job.followers = res.CommonPrefixes
		}
		out <- job
	}
}

var jobIDCounter uint64

type Job struct {
	id        uint64
	path      string
	duration  time.Duration
	retryLeft int
	err       error
	keys      []s3.Key
	followers []string
}

func newJob(rel string) *Job {
	var relpath string
	if path.IsAbs(rel) {
		relpath = rel[1:]
	} else {
		relpath = rel
	}
	return &Job{
		id:        atomic.AddUint64(&jobIDCounter, 1),
		retryLeft: 5,
		path:      relpath,
		// other fields nil-value is correct
	}
}

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

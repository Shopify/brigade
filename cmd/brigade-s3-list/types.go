package main

import (
	"github.com/bmizerany/perks/quantile"
	"github.com/crowdmob/goamz/s3"
	"github.com/dustin/go-humanize"
	"log"
	"path"
	"runtime"
	"sync/atomic"
	"time"
)

// Collections

type jobsColl interface {
	IsEmpty() bool
	Add(*Job)
	Remove() *Job
	Peek() *Job
	Len() int
}

var (
	// Compile checks
	_ jobsColl = &fifoJobs{}
	_ jobsColl = &lifoJobs{}
)

type fifoJobs struct{ queue []*Job }

func (f *fifoJobs) IsEmpty() bool { return len(f.queue) == 0 }
func (f *fifoJobs) Len() int      { return len(f.queue) }
func (f *fifoJobs) Add(job *Job)  { f.queue = append(f.queue, job) }
func (f *fifoJobs) Peek() *Job    { return f.queue[0] }
func (f *fifoJobs) Remove() *Job {
	item := f.queue[0]
	f.queue = f.queue[1:]
	return item
}

type lifoJobs struct{ stack []*Job }

func (l *lifoJobs) IsEmpty() bool { return len(l.stack) == 0 }
func (l *lifoJobs) Len() int      { return len(l.stack) }
func (l *lifoJobs) Add(job *Job)  { l.stack = append(l.stack, job) }
func (l *lifoJobs) Peek() *Job    { return l.stack[len(l.stack)-1] }
func (l *lifoJobs) Remove() *Job {
	item := l.stack[len(l.stack)-1]
	l.stack = l.stack[0 : len(l.stack)-1]
	return item
}

type jobSet map[uint64]struct{}

func (j jobSet) Contains(job *Job) bool { _, ok := j[job.id]; return ok }
func (j jobSet) IsEmpty() bool          { return len(j) == 0 }
func (j jobSet) Add(job *Job)           { j[job.id] = struct{}{} }
func (j jobSet) Delete(job *Job)        { delete(j, job.id) }

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

// stats to print the progress of a bucket walk
type walkStats struct {
	newkeys    int64
	newleads   int64
	totalkeys  int64
	jobspersec int64
	totalsize  uint64
	mem        runtime.MemStats
	qtstream   *quantile.Stream
}

func newWalkStats() *walkStats {
	return &walkStats{
		qtstream: quantile.NewTargeted(0.50, 0.95),
	}
}

func (w *walkStats) printProgress(workset jobSet, followers jobsColl) {
	curInflight := atomic.LoadInt64(&inflight)

	runtime.ReadMemStats(&w.mem)
	p50 := time.Duration(int64(w.qtstream.Query(0.50)))
	p95 := time.Duration(int64(w.qtstream.Query(0.95)))

	log.Printf("mem=%s\tjobs/s=%s\twork=%s\tfollow=%s\tinflight=%s\tkeys=%s\tbktsize=%s\tnew=%s\tleads=%s\tp50=%v\tp95=%v",
		humanize.Bytes(w.mem.Sys-w.mem.HeapReleased),
		humanize.Comma(w.jobspersec),
		humanize.Comma(int64(len(workset))),
		humanize.Comma(int64(followers.Len())),
		humanize.Comma(curInflight),
		humanize.Comma(w.totalkeys),
		humanize.Bytes(w.totalsize),
		humanize.Comma(w.newkeys),
		humanize.Comma(w.newleads),
		p50,
		p95)
	w.newkeys = 0
	w.newleads = 0
	w.jobspersec = 0
	w.qtstream.Reset()
}

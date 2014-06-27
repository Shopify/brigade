package list

import (
	"bytes"
	"fmt"
	"github.com/aybabtme/goamz/s3"
	"github.com/bmizerany/perks/quantile"
	"github.com/dustin/go-humanize"
	"log"
	"path"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Collections

type lifoJobs struct{ stack []*Job }

func (l *lifoJobs) IsEmpty() bool { return len(l.stack) == 0 }
func (l *lifoJobs) Len() int      { return len(l.stack) }
func (l *lifoJobs) Add(job *Job)  { l.stack = append(l.stack, job) }
func (l *lifoJobs) Peek() *Job    { return l.stack[len(l.stack)-1] }
func (l *lifoJobs) Remove() *Job {
	item := l.stack[len(l.stack)-1]
	l.stack = l.stack[:len(l.stack)-1]
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
	sync.Mutex

	workToDo  int64
	followers int64

	newKeys    int64
	newLeads   int64
	totalKeys  int64
	jobsPerSec int64
	totalSize  uint64
	mem        runtime.MemStats
	qtStream   *quantile.Stream

	tick *time.Ticker
}

func newWalkStats() *walkStats {
	stats := &walkStats{
		qtStream: quantile.NewTargeted(0.50, 0.95),
		tick:     time.NewTicker(time.Second),
	}

	go func() {
		for _ = range stats.tick.C {
			stats.Lock()
			stats.printProgress()
			stats.Unlock()
		}
	}()

	return stats
}

func (w *walkStats) Stop() { w.tick.Stop() }

func (w *walkStats) printProgress() {
	curInflight := atomic.LoadInt64(&inflight)

	runtime.ReadMemStats(&w.mem)
	p50 := time.Duration(int64(w.qtStream.Query(0.50)))
	p95 := time.Duration(int64(w.qtStream.Query(0.95)))

	buf := bytes.NewBuffer(nil)
	_, _ = fmt.Fprintf(buf, "mem=%s\t", humanize.Bytes(w.mem.Sys-w.mem.HeapReleased))
	_, _ = fmt.Fprintf(buf, "jobs/s=%s\t", humanize.Comma(w.jobsPerSec))
	_, _ = fmt.Fprintf(buf, "work=%s\t", humanize.Comma(w.workToDo))
	_, _ = fmt.Fprintf(buf, "follow=%s\t", humanize.Comma(w.followers))
	_, _ = fmt.Fprintf(buf, "inflight=%s\t", humanize.Comma(curInflight))
	_, _ = fmt.Fprintf(buf, "keys=%s\t", humanize.Comma(w.totalKeys))
	_, _ = fmt.Fprintf(buf, "bktsize=%s\t", humanize.Bytes(w.totalSize))
	_, _ = fmt.Fprintf(buf, "new=%s\t", humanize.Comma(w.newKeys))
	_, _ = fmt.Fprintf(buf, "leads=%s\t", humanize.Comma(w.newLeads))
	_, _ = fmt.Fprintf(buf, "p50=%v\t", p50)
	_, _ = fmt.Fprintf(buf, "p95=%v\t", p95)
	log.Println(buf.String())

	w.newKeys = 0
	w.newLeads = 0
	w.jobsPerSec = 0
	w.qtStream.Reset()
}

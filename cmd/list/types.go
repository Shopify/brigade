package list

import (
	"github.com/aybabtme/goamz/s3"
	"path"
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

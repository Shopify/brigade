package sync

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/aybabtme/goamz/s3"
	"github.com/bmizerany/perks/quantile"
	"github.com/dustin/go-humanize"
	"io"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	targetP50 = 0.50
	targetP95 = 0.95

	// PutCopy method of syncing buckets, the sync is done entirely within
	// S3.
	PutCopy = "putcopy"
)

var (
	q = struct{}{}

	// BufferFactor of decode/sync channels,
	// which are BufferFactor-times bigger than their
	// parallelism.
	BufferFactor = 10

	method = map[string]func(*s3.Bucket, *s3.Bucket, s3.Key) error{

		PutCopy: func(src, dst *s3.Bucket, key s3.Key) error {
			_, err := dst.PutCopy(key.Key, s3.Private, s3.CopyOptions{}, src.Name+"/"+key.Key)
			return err
		},
	}
)

// Options for bucket syncing.
type Options struct {
	RetryBase time.Duration

	MaxRetry   int
	DecodePara int
	SyncPara   int

	MethodName string
}

func (o *Options) setDefaults() {
	if o.MaxRetry <= 0 {
		// will fail to retry ~9.8 times out of 10'000 if 50% of calls
		// return retryable errors that would otherwise eventually
		// have succeeded
		o.MaxRetry = 10
	}
	if o.DecodePara <= 0 {
		o.DecodePara = runtime.NumCPU()
	}
	if o.SyncPara <= 0 {
		o.DecodePara = 200
	}
	if o.RetryBase == time.Duration(0) {
		o.RetryBase = time.Second
	}
}

// Sync creates and starts a sync task, reading all the keys that need to be sync'd
// from the input reader, in JSON form, copying the keys in src onto dst.
func Sync(el *log.Logger, input io.Reader, success, fail io.Writer, src, dst *s3.Bucket, opts Options) error {
	opts.setDefaults()

	// before starting the sync, make sure our s3 object is usable (credentials and such)
	_, err := dst.List("/", "/", "/", 1)
	if err != nil {
		// if we can't list, we abort right away
		return fmt.Errorf("couldn't list destination bucket %q: %v", dst.Name, err)
	}

	task := syncTask{
		elog:     el,
		src:      src,
		dst:      dst,
		qtStream: quantile.NewTargeted(targetP50, targetP95),
		opts:     opts,
	}

	if syncMethod, ok := method[opts.MethodName]; ok {
		task.syncMethod = syncMethod
	} else {
		task.syncMethod = method[PutCopy]
	}

	return task.Start(input, success, fail)
}

type syncTask struct {
	elog *log.Logger

	src *s3.Bucket
	dst *s3.Bucket

	qtStreamL sync.Mutex
	qtStream  *quantile.Stream

	syncMethod func(src, dst *s3.Bucket, key s3.Key) error

	opts Options

	// shared stats between goroutines, use sync/atomic
	fileLines   int64
	decodedKeys int64
	syncedKeys  int64
	inflight    int64
}

func (s *syncTask) Start(input io.Reader, synced, failed io.Writer) error {
	start := time.Now()

	ticker := time.NewTicker(time.Second)
	go s.printProgress(ticker)

	keysIn := make(chan s3.Key, s.opts.SyncPara*BufferFactor)
	keysOk := make(chan s3.Key, s.opts.SyncPara*BufferFactor)
	keysFail := make(chan s3.Key, s.opts.SyncPara*BufferFactor)

	decoders := make(chan []byte, s.opts.DecodePara*BufferFactor)

	// start JSON decoders
	log.Printf("starting %d key decoders, buffer size %d", s.opts.DecodePara, cap(decoders))
	decGroup := sync.WaitGroup{}
	for i := 0; i < s.opts.DecodePara; i++ {
		decGroup.Add(1)
		go s.decode(&decGroup, decoders, keysIn)
	}

	// start S3 sync workers
	log.Printf("starting %d key sync workers, buffer size %d", s.opts.SyncPara, cap(keysIn))
	syncGroup := sync.WaitGroup{}
	for i := 0; i < s.opts.SyncPara; i++ {
		syncGroup.Add(1)
		go s.syncKey(&syncGroup, s.src, s.dst, keysIn, keysOk, keysFail)
	}

	// track keys that have been sync'd, and those that we failed to sync.
	log.Printf("starting to write progress")
	encGroup := sync.WaitGroup{}
	encGroup.Add(2)
	go s.encode(&encGroup, synced, keysOk)
	go s.encode(&encGroup, failed, keysFail)

	// feed the pipeline by reading the listing file
	log.Printf("starting to read key listing file")
	err := s.readLines(input, decoders)

	// when done reading the source file, wait until the decoders
	// are done.
	log.Printf("done reading %s lines in %v",
		humanize.Comma(atomic.LoadInt64(&s.fileLines)),
		time.Since(start))
	close(decoders)
	decGroup.Wait()

	// when the decoders are all done, wait for the sync workers to finish
	log.Printf("done decoding %s keys in %v",
		humanize.Comma(atomic.LoadInt64(&s.decodedKeys)),
		time.Since(start))

	close(keysIn)
	syncGroup.Wait()

	close(keysOk)
	close(keysFail)

	encGroup.Wait()

	ticker.Stop()

	// the source file is read, all keys were decoded and sync'd. we're done.
	log.Printf("done syncing %s keys in %v",
		humanize.Comma(atomic.LoadInt64(&s.syncedKeys)),
		time.Since(start))

	return err
}

// prints progress and stats as we go, handy to figure out what's going on
// and how the tool performs.
func (s *syncTask) printProgress(tick *time.Ticker) {
	for _ = range tick.C {
		s.qtStreamL.Lock()
		p50, p95 := s.qtStream.Query(targetP50), s.qtStream.Query(targetP95)
		s.qtStream.Reset()
		s.qtStreamL.Unlock()

		log.Printf("fileLines=%s\tdecodedKeys=%s\tsyncedKeys=%s\tinflight=%d/%d\tsync-p50=%v\tsync-p95=%v",
			humanize.Comma(s.fileLines),
			humanize.Comma(s.decodedKeys),
			humanize.Comma(s.syncedKeys),
			atomic.LoadInt64(&s.inflight), s.opts.SyncPara,
			time.Duration(p50),
			time.Duration(p95),
		)
	}
}

// reads all the \n separated lines from a file, write them (without \n) to
// the channel. reads until EOF or stops on the first error encountered
func (s *syncTask) readLines(input io.Reader, decoders chan<- []byte) error {

	rd := bufio.NewReader(input)

	for {
		line, err := rd.ReadBytes('\n')
		switch err {
		case io.EOF:
			return nil
		case nil:
		default:
			return err
		}

		decoders <- line
		atomic.AddInt64(&s.fileLines, 1)
	}
}

// decodes s3.Keys from a channel of bytes, each byte containing a full key
func (s *syncTask) decode(wg *sync.WaitGroup, lines <-chan []byte, keys chan<- s3.Key) {
	defer wg.Done()
	var key s3.Key
	for line := range lines {
		err := json.Unmarshal(line, &key)
		if err != nil {
			s.elog.Printf("unmarshaling line: %v", err)
		} else {
			keys <- key
			atomic.AddInt64(&s.decodedKeys, 1)
		}
	}
}

// encode write the keys it receives in JSON to a dst writer.
func (s *syncTask) encode(wg *sync.WaitGroup, dst io.Writer, keys <-chan s3.Key) {
	defer wg.Done()
	enc := json.NewEncoder(dst)
	for key := range keys {
		err := enc.Encode(key)
		if err != nil {
			s.elog.Fatalf("encoding %q to JSON: %v", key.Key, err)
		}
	}
}

// syncKey uses s.syncMethod to copy keys from `src` to `dst`, until `keys` is
// closed. Each key error is retried MaxRetry times, unless the error is not
// retryable.
func (s *syncTask) syncKey(wg *sync.WaitGroup, src, dst *s3.Bucket, keys <-chan s3.Key, synced, failed chan<- s3.Key) {
	defer wg.Done()

	var err error
	for key := range keys {
		// `retry` is used outside the loop, starts at 1
		retry := 1
	retrying:
		for ; retry <= s.opts.MaxRetry; retry++ {
			start := time.Now()

			// do a put copy call (sync directly from bucket to another
			// without fetching the content locally)
			atomic.AddInt64(&s.inflight, 1)
			err = s.syncMethod(src, dst, key)
			atomic.AddInt64(&s.inflight, -1)
			s.qtStreamL.Lock()
			s.qtStream.Insert(float64(time.Since(start).Nanoseconds()))
			s.qtStreamL.Unlock()

			switch e := err.(type) {
			case nil:
				// when there are no errors, there's nothing to retry
				break retrying
			case *s3.Error:
				// if the error is specific to S3, we can do smart stuff like
				if shouldAbort(e) {
					// abort if its an error that will occur for all future calls
					// such as bad auth, or the bucket not existing anymore (that'd be bad!)
					s.elog.Fatalf("abort-worthy-error=%q\terror-msg=%q\tkey=%#v", e.Code, e.Message, key)
				}
				if !shouldRetry(e) {
					// give up on that key if it's not retryable, such as a key
					// that was deleted
					s.elog.Printf("unretriable-error=%q\terror-msg=%q\tkey=%q", e.Code, e.Message, key.Key)
					break retrying
				}
				// carry on to retry
			default:
				// carry on to retry
			}

			// log that we sleep, but don't log the error itself just
			// yet (to avoid logging transient network errors that are
			// recovered by retrying)
			sleepFor := s.opts.RetryBase * time.Duration(retry)
			s.elog.Printf("worker-sleep-on-retryiable-error=%v", sleepFor)
			time.Sleep(sleepFor)
			s.elog.Printf("worker-wake-up, retries=%d/%d", retry, s.opts.MaxRetry)

		}

		// If we exhausted MaxRetry, log the error to the error log
		if err != nil {
			failed <- key
			s.elog.Printf("failed %d times to sync %q", retry, key.Key)
			switch e := err.(type) {
			case *s3.Error:
				if shouldAbort(e) {
					s.elog.Fatalf("abort-worthy-error=%#v\tkey=%#v", e, key)
				}
				s.elog.Printf("s3-error-code=%q\ts3-error-msg=%q\tkey=%q", e.Code, e.Message, key.Key)
			default:
				s.elog.Printf("other-error=%#v\tkey=%q", e, key.Key)
			}
			continue
		}

		synced <- key
		atomic.AddInt64(&s.syncedKeys, 1)
	}
}

// Classify S3 errors that should be retried.
func shouldRetry(err error) bool {
	switch {
	default:
		// don't retry errors
		return false

		// unless they're one of:
	case s3.IsS3Error(err, s3.ErrExpiredToken):
	case s3.IsS3Error(err, s3.ErrIncompleteBody):
	case s3.IsS3Error(err, s3.ErrInternalError):
	case s3.IsS3Error(err, s3.ErrInvalidBucketState):
	case s3.IsS3Error(err, s3.ErrInvalidObjectState):
	case s3.IsS3Error(err, s3.ErrInvalidPart):
	case s3.IsS3Error(err, s3.ErrInvalidPartOrder):
	case s3.IsS3Error(err, s3.ErrOperationAborted):
	case s3.IsS3Error(err, s3.ErrPermanentRedirect):
	case s3.IsS3Error(err, s3.ErrPreconditionFailed):
	case s3.IsS3Error(err, s3.ErrRedirect):
	case s3.IsS3Error(err, s3.ErrRequestTimeout):
	case s3.IsS3Error(err, s3.ErrRequestTimeTooSkewed):
	case s3.IsS3Error(err, s3.ErrServiceUnavailable):
	case s3.IsS3Error(err, s3.ErrTemporaryRedirect):
	case s3.IsS3Error(err, s3.ErrTokenRefreshRequired):
	case s3.IsS3Error(err, s3.ErrUnexpectedContent):
	case s3.IsS3Error(err, s3.ErrSlowDown):
	}
	return true
}

// Classify S3 errors that require aborting the whole sync process.
func shouldAbort(err error) bool {
	switch {
	default:
		// don't abort on errors
		return false

		// unless they're one of:
	case s3.IsS3Error(err, s3.ErrAccessDenied):
	case s3.IsS3Error(err, s3.ErrAccountProblem):
	case s3.IsS3Error(err, s3.ErrCredentialsNotSupported):
	case s3.IsS3Error(err, s3.ErrInvalidAccessKeyID):
	case s3.IsS3Error(err, s3.ErrInvalidBucketName):
	case s3.IsS3Error(err, s3.ErrNoSuchBucket):
	}
	return true
}

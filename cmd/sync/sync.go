package sync

import (
	"bufio"
	"encoding/json"
	"github.com/Shopify/brigade/s3util"
	"github.com/bmizerany/perks/quantile"
	"github.com/crowdmob/goamz/s3"
	"github.com/dustin/go-humanize"
	"io"
	"log"
	"net/url"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// BufferFactor of decode/sync channels,
	// which are BufferFactor-times bigger than their
	// parallelism.
	BufferFactor = 10

	targetP50 = 0.50
	targetP95 = 0.95

	q    = struct{}{}
	elog *log.Logger

	// PutCopy method of syncing buckets, the sync is done entirely within
	// S3.
	PutCopy = "putcopy"
)

var method = map[string]func(*s3.Bucket, *s3.Bucket, s3.Key) error{

	PutCopy: func(src, dst *s3.Bucket, key s3.Key) error {
		_, err := dst.PutCopy(key.Key, s3.Private, s3.CopyOptions{}, src.Name+"/"+key.Key)
		return err
	},
}

// Options for bucket syncing.
type Options struct {
	MaxRetry   int
	DecodePara int
	SyncPara   int

	MethodName string
}

func (o *Options) setDefaults() {
	if o.MaxRetry < 0 {
		o.MaxRetry = 5
	}
	if o.DecodePara <= 0 {
		o.DecodePara = runtime.NumCPU()
	}
	if o.SyncPara <= 0 {
		o.DecodePara = 200
	}
}

// Sync creates and starts a sync task, reading all the keys that need to be sync'd
// from the input reader, in JSON form, copying the keys in src onto dst.
func Sync(el *log.Logger, input io.Reader, sss *s3.S3, src, dst *url.URL, opts Options) {
	elog = el
	opts.setDefaults()

	task := syncTask{
		sss:      sss,
		src:      src,
		dst:      dst,
		qtstream: quantile.NewTargeted(targetP50, targetP95),
		opts:     opts,
	}

	if syncMethod, ok := method[opts.MethodName]; ok {
		task.syncMethod = syncMethod
	} else {
		task.syncMethod = method[PutCopy]
	}

	task.Start(input)
}

type syncTask struct {
	sss *s3.S3
	src *url.URL
	dst *url.URL

	qtstreamL sync.Mutex
	qtstream  *quantile.Stream

	syncMethod func(src, dst *s3.Bucket, key s3.Key) error

	opts Options

	// shared stats between goroutines, use sync/atomic
	filelines   int64
	decodedKeys int64
	syncedKeys  int64
	inflight    int64
}

func (s *syncTask) Start(input io.Reader) {
	start := time.Now()

	ticker := time.NewTicker(time.Second)
	go s.printProgress(ticker)

	keys := make(chan s3.Key, s.opts.SyncPara*BufferFactor)
	decoders := make(chan []byte, s.opts.DecodePara*BufferFactor)

	// start JSON decoders
	log.Printf("starting %d key decoders, buffer size %d", s.opts.DecodePara, cap(decoders))
	decGroup := sync.WaitGroup{}
	for i := 0; i < s.opts.DecodePara; i++ {
		decGroup.Add(1)
		go s.decode(&decGroup, decoders, keys)
	}

	// start S3 sync workers
	log.Printf("starting %d key sync workers, buffer size %d", s.opts.SyncPara, cap(keys))
	syncGroup := sync.WaitGroup{}
	for i := 0; i < s.opts.SyncPara; i++ {
		syncGroup.Add(1)
		go s.syncKey(&syncGroup, s.sss, s.src, s.dst, keys)
	}

	// feed the pipeline by reading the listing file
	log.Printf("starting to read key listing file")
	s.readLines(input, decoders)

	// when done reading the source file, wait until the decoders
	// are done.
	log.Printf("done reading %s lines in %v",
		humanize.Comma(atomic.LoadInt64(&s.filelines)),
		time.Since(start))
	close(decoders)
	decGroup.Wait()

	// when the decoders are all done, wait for the sync workers to finish
	log.Printf("done decoding %s keys in %v",
		humanize.Comma(atomic.LoadInt64(&s.decodedKeys)),
		time.Since(start))

	close(keys)
	syncGroup.Wait()
	ticker.Stop()

	// the source file is read, all keys were decoded and sync'd. we're done.
	log.Printf("done syncing %s keys in %v",
		humanize.Comma(atomic.LoadInt64(&s.syncedKeys)),
		time.Since(start))
}

// prints progress and stats as we go, handy to figure out what's going on
// and how the tool performs.
func (s *syncTask) printProgress(tick *time.Ticker) {
	for _ = range tick.C {
		s.qtstreamL.Lock()
		p50, p95 := s.qtstream.Query(targetP50), s.qtstream.Query(targetP95)
		s.qtstream.Reset()
		s.qtstreamL.Unlock()

		log.Printf("filelines=%s\tdecodedKeys=%s\tsyncedKeys=%s\tinflight=%d/%d\tsync-p50=%v\tsync-p95=%v",
			humanize.Comma(s.filelines),
			humanize.Comma(s.decodedKeys),
			humanize.Comma(s.syncedKeys),
			atomic.LoadInt64(&s.inflight), s.opts.SyncPara,
			time.Duration(p50),
			time.Duration(p95),
		)
	}
}

// reads all the \n separated lines from a file
func (s *syncTask) readLines(input io.Reader, decoders chan<- []byte) {

	rd := bufio.NewReader(input)

	for {
		line, err := rd.ReadBytes('\n')
		switch err {
		case io.EOF:
			return
		case nil:
		default:
			elog.Fatal(err)
		}

		decoders <- line
		atomic.AddInt64(&s.filelines, 1)
	}
}

// decodes s3.Keys from a channel of bytes, each byte containing a full key
func (s *syncTask) decode(wg *sync.WaitGroup, lines <-chan []byte, keys chan<- s3.Key) {
	defer wg.Done()
	var key s3.Key
	for line := range lines {
		err := json.Unmarshal(line, &key)
		if err != nil {
			elog.Printf("unmarshaling line: %v", err)
		} else {
			keys <- key
			atomic.AddInt64(&s.decodedKeys, 1)
		}
	}
}

func (s *syncTask) syncKey(wg *sync.WaitGroup, sss *s3.S3, src, dst *url.URL, keys <-chan s3.Key) {
	defer wg.Done()

	srcBkt := sss.Bucket(src.Host)
	dstBkt := sss.Bucket(dst.Host)
	// before starting the sync, make sure our s3 object is usable (credentials and such)
	_, err := dstBkt.List("/", "/", "/", 1)
	if err != nil {
		// if we can't list, we abort right away
		elog.Fatalf("couldn't list destination bucket %q: %v", dst, err)
	}

	for key := range keys {
		retry := 1
	retrying:
		for ; retry <= s.opts.MaxRetry; retry++ {
			start := time.Now()

			// do a put copy call (sync directly from bucket to another
			// without fetching the content locally)
			atomic.AddInt64(&s.inflight, 1)
			err := s.syncMethod(srcBkt, dstBkt, key)
			atomic.AddInt64(&s.inflight, -1)
			s.qtstreamL.Lock()
			s.qtstream.Insert(float64(time.Since(start).Nanoseconds()))
			s.qtstreamL.Unlock()

			switch e := err.(type) {
			case nil:
				// when there are no errors, there's nothing to retry
				break retrying
			case *s3.Error:
				// if the error is specific to S3, we can do smart stuff like
				if shouldAbort(e) {
					// abort if its an error that will occur for all future calls
					// such as bad auth, or the bucket not existing anymore
					elog.Fatalf("abort-worthy-error=%q\terror-msg=%q\tkey=%#v", e.Code, e.Message, key)
				}
				if !shouldRetry(e) {
					// give up on that key if it's not retryable, such as a key
					// that was deleted
					elog.Printf("unretriable-error=%q\terror-msg=%q\tkey=%q", e.Code, e.Message, key.Key)
					break retrying
				}
				// continue to retry
			default:
				// continue to retry
			}

			// log that we sleep, but don't log the error itself just
			// yet (to avoid logging transient network errors that are
			// recovered by retrying)
			sleepFor := time.Second * time.Duration(retry)
			elog.Printf("worker-sleep-on-retryiable-error=%v", sleepFor)
			time.Sleep(sleepFor)
			elog.Printf("worker-wake-up, retries=%d/%d", retry, s.opts.MaxRetry)

		}

		// If we exhausted MaxRetry, log the error to the output
		if err != nil {
			elog.Printf("failed %d times to sync %q", retry, key.Key)
			switch e := err.(type) {
			case *s3.Error:
				if shouldAbort(e) {
					elog.Fatalf("abort-worthy-error=%#v\tkey=%#v", e, key)
				}
				elog.Printf("s3-error-code=%q\ts3-error-msg=%q\tkey=%q", e.Code, e.Message, key.Key)
			default:
				elog.Printf("other-error=%#v\tkey=%q", e, key.Key)
			}
			continue
		}

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
	case s3util.IsS3Error(err, s3util.ErrExpiredToken):
	case s3util.IsS3Error(err, s3util.ErrIncompleteBody):
	case s3util.IsS3Error(err, s3util.ErrInternalError):
	case s3util.IsS3Error(err, s3util.ErrInvalidBucketState):
	case s3util.IsS3Error(err, s3util.ErrInvalidObjectState):
	case s3util.IsS3Error(err, s3util.ErrInvalidPart):
	case s3util.IsS3Error(err, s3util.ErrInvalidPartOrder):
	case s3util.IsS3Error(err, s3util.ErrOperationAborted):
	case s3util.IsS3Error(err, s3util.ErrPermanentRedirect):
	case s3util.IsS3Error(err, s3util.ErrPreconditionFailed):
	case s3util.IsS3Error(err, s3util.ErrRedirect):
	case s3util.IsS3Error(err, s3util.ErrRequestTimeout):
	case s3util.IsS3Error(err, s3util.ErrRequestTimeTooSkewed):
	case s3util.IsS3Error(err, s3util.ErrServiceUnavailable):
	case s3util.IsS3Error(err, s3util.ErrTemporaryRedirect):
	case s3util.IsS3Error(err, s3util.ErrTokenRefreshRequired):
	case s3util.IsS3Error(err, s3util.ErrUnexpectedContent):
	case s3util.IsS3Error(err, s3util.ErrSlowDown):
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
	case s3util.IsS3Error(err, s3util.ErrAccessDenied):
	case s3util.IsS3Error(err, s3util.ErrAccountProblem):
	case s3util.IsS3Error(err, s3util.ErrCredentialsNotSupported):
	case s3util.IsS3Error(err, s3util.ErrInvalidAccessKeyId):
	case s3util.IsS3Error(err, s3util.ErrInvalidBucketName):
	case s3util.IsS3Error(err, s3util.ErrNoSuchBucket):
	}
	return true
}

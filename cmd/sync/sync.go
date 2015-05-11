package sync

import (
	"bufio"
	"encoding/json"
	"expvar"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/pushrax/goamz/s3"
	"io"
	"runtime"
	"sync"
	"time"
)

const (
	targetP50 = 0.50
	targetP95 = 0.95
)

var (
	q = struct{}{}

	// BufferFactor of decode/sync channels,
	// which are BufferFactor-times bigger than their
	// parallelism.
	BufferFactor = 10
)

// SyncerFunc syncs an s3.Key from a source to a destination bucket.
type SyncerFunc func(src *s3.Bucket, dst *s3.Bucket, key s3.Key) error

// PutCopySyncer does a PutCopy call to S3, copying a key from src to dst
// if both are in the same region.
func PutCopySyncer(src, dst *s3.Bucket, key s3.Key) error {
	_, err := dst.PutCopy(key.Key, ACLForKey(src, key), s3.CopyOptions{}, src.Name+"/"+key.Key)
	return err
}

// GetPutSyncer does a GET, then a PUT on the key, streaming the GET reader
// to the PUT writer with a buffer.
func GetPutSyncer(src, dst *s3.Bucket, key s3.Key) error {
	rd, err := src.GetReader(key.Key)
	if err != nil {
		return err
	}
	bufrd := bufio.NewReader(rd)
	return src.PutReader(key.Key, bufrd, key.Size, "", ACLForKey(src, key), s3.Options{})
}

var ACLForKey func(bkt *s3.Bucket, k s3.Key) s3.ACL = S3ACLForKey

func MockACLForKey(bkt *s3.Bucket, k s3.Key) s3.ACL {
	return s3.PublicRead
}

func S3ACLForKey(bkt *s3.Bucket, k s3.Key) s3.ACL {
	if keyIsPrivate(bkt, k) {
		return s3.Private
	}

	return s3.PublicRead
}

func keyIsPrivate(bkt *s3.Bucket, k s3.Key) bool {
	resp, err := bkt.GetPermissions(k.Key)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"key":   k,
			"error": err,
		}).Warn("couldn't get acl for key")
		return false
	}

	for _, grant := range resp.AccessControlList {
		if grant.Grantee.URI == "http://acs.amazonaws.com/groups/global/AllUsers" {
			return grant.Permission != "READ"
		}
	}
	return true
}

// NewSyncTask creates a sync task that will sync keys from src onto dst.
func NewSyncTask(src, dst *s3.Bucket) (*SyncTask, error) {

	// before starting the sync, make sure our s3 object is usable (credentials and such)
	_, err := src.List("/", "/", "/", 1)
	if err != nil {
		// if we can't list, we abort right away
		return nil, fmt.Errorf("couldn't list source bucket %q: %v", src.Name, err)
	}
	_, err = dst.List("/", "/", "/", 1)
	if err != nil {
		return nil, fmt.Errorf("couldn't list destination bucket %q: %v", dst.Name, err)
	}

	return &SyncTask{
		RetryBase:  time.Second,
		MaxRetry:   20,
		DecodePara: runtime.NumCPU(),
		SyncPara:   1000,
		Sync:       PutCopySyncer,

		src: src,
		dst: dst,
	}, nil
}

// SyncTask synchronizes keys between two buckets.
type SyncTask struct {
	RetryBase  time.Duration
	MaxRetry   int
	DecodePara int
	SyncPara   int
	Sync       SyncerFunc

	src *s3.Bucket
	dst *s3.Bucket
}

var metrics = struct {
	fileLines   *expvar.Int
	decodedKeys *expvar.Int

	inflight         *expvar.Int
	secondsWaitingS3 *expvar.Float

	syncAttempted *expvar.Int
	syncOk        *expvar.Int
	syncRetries   *expvar.Int
	syncAbandoned *expvar.Int
}{
	fileLines:   expvar.NewInt("brigade.sync.fileLines"),
	decodedKeys: expvar.NewInt("brigade.sync.decodedKeys"),

	inflight:         expvar.NewInt("brigade.sync.inflight"),
	secondsWaitingS3: expvar.NewFloat("brigade.sync.secondsWaitingS3"),

	syncAttempted: expvar.NewInt("brigade.sync.syncAttempted"),
	syncOk:        expvar.NewInt("brigade.sync.syncOk"),
	syncRetries:   expvar.NewInt("brigade.sync.syncRetries"),
	syncAbandoned: expvar.NewInt("brigade.sync.syncAbandoned"),
}

// Start the task, reading all the keys that need to be sync'd
// from the input reader, in JSON form, copying the keys in src onto dst.
func (s *SyncTask) Start(input io.Reader, synced, failed io.Writer) error {

	start := time.Now()

	keysIn := make(chan s3.Key, s.SyncPara*BufferFactor)
	keysOk := make(chan s3.Key, s.SyncPara*BufferFactor)
	keysFail := make(chan s3.Key, s.SyncPara*BufferFactor)

	decoders := make(chan []byte, s.DecodePara*BufferFactor)

	// start JSON decoders
	logrus.WithFields(logrus.Fields{
		"key_decoders": s.DecodePara,
		"buffer_size":  cap(decoders),
	}).Info("starting key decoders")

	decGroup := sync.WaitGroup{}
	for i := 0; i < s.DecodePara; i++ {
		decGroup.Add(1)
		go s.decode(&decGroup, decoders, keysIn)
	}

	// start S3 sync workers
	logrus.WithFields(logrus.Fields{
		"sync_workers": s.SyncPara,
		"buffer_size":  cap(keysIn),
	}).Info("starting key sync workers")
	syncGroup := sync.WaitGroup{}
	for i := 0; i < s.SyncPara; i++ {
		syncGroup.Add(1)
		go s.syncKey(&syncGroup, s.src, s.dst, keysIn, keysOk, keysFail)
	}

	// track keys that have been sync'd, and those that we failed to sync.
	logrus.Info("starting to write progress")
	encGroup := sync.WaitGroup{}
	encGroup.Add(2)
	go s.encode(&encGroup, synced, keysOk)
	go s.encode(&encGroup, failed, keysFail)

	// feed the pipeline by reading the listing file
	logrus.Info("starting to read key listing file")
	err := s.readLines(input, decoders)

	// when done reading the source file, wait until the decoders
	// are done.
	logrus.WithFields(logrus.Fields{
		"since_start": time.Since(start),
		"line_count":  metrics.fileLines.String(),
	}).Info("done reading lines from sync list")
	close(decoders)
	decGroup.Wait()

	// when the decoders are all done, wait for the sync workers to finish

	logrus.WithFields(logrus.Fields{
		"since_start": time.Since(start),
		"line_count":  metrics.decodedKeys.String(),
	}).Info("done decoding keys from sync list")

	close(keysIn)
	syncGroup.Wait()

	close(keysOk)
	close(keysFail)

	encGroup.Wait()

	// the source file is read, all keys were decoded and sync'd. we're done.
	logrus.WithFields(logrus.Fields{
		"since_start": time.Since(start),
		"sync_ok":     metrics.syncOk.String(),
		"sync_fail":   metrics.syncAbandoned.String(),
	}).Info("done syncing keys")

	return err
}

// reads all the \n separated lines from a file, write them (without \n) to
// the channel. reads until EOF or stops on the first error encountered
func (s *SyncTask) readLines(input io.Reader, decoders chan<- []byte) error {

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
		metrics.fileLines.Add(1)
	}
}

// decodes s3.Keys from a channel of bytes, each byte containing a full key
func (s *SyncTask) decode(wg *sync.WaitGroup, lines <-chan []byte, keys chan<- s3.Key) {
	defer wg.Done()
	var key s3.Key
	for line := range lines {
		err := json.Unmarshal(line, &key)
		if err != nil {
			logrus.WithField("error", err).Fatal("failed to unmarshal s3.Key from line")
		} else {
			keys <- key
			metrics.decodedKeys.Add(1)
		}
	}
}

// encode write the keys it receives in JSON to a dst writer.
func (s *SyncTask) encode(wg *sync.WaitGroup, dst io.Writer, keys <-chan s3.Key) {
	defer wg.Done()
	enc := json.NewEncoder(dst)
	for key := range keys {
		err := enc.Encode(key)
		if err != nil {
			// panic so that someone come look at why the destination can't be
			// written to, with a stack trace to help
			logrus.WithFields(logrus.Fields{
				"error": err,
				"key":   key,
			}).Panic("failed to encode s3.Key to output, bailing")
		}
	}
}

// syncKey uses s.syncMethod to copy keys from `src` to `dst`, until `keys` is
// closed. Each key error is retried MaxRetry times, unless the error is not
// retriable.
func (s *SyncTask) syncKey(wg *sync.WaitGroup, src, dst *s3.Bucket, keys <-chan s3.Key, synced, failed chan<- s3.Key) {
	defer wg.Done()

	for key := range keys {
		retries, err := s.syncOrRetry(src, dst, key)
		// If we exhausted MaxRetry, log the error to the error log
		if err != nil {
			metrics.syncAbandoned.Add(1)
			failed <- key

			entry := logrus.WithFields(logrus.Fields{
				"retries": retries,
				"key":     key,
				"error":   err,
			})

			switch e := err.(type) {
			case *s3.Error: // cannot be abort worthy at this point
				entry.WithFields(logrus.Fields{
					"s3_code":    e.Code,
					"s3_message": e.Message,
				}).Error("failed too many times to sync key, abandoned: s3.Error")
			default:
				entry.Error("failed too many times to sync key, abandoned: unexpected error")
			}

		} else {
			metrics.syncOk.Add(1)
			synced <- key
		}
	}
}

// syncOrRetry will try to sync a key many times, until it succeeds or
// fail more than MaxRetry times. It will sleep between retries and abort
// the program on errors that are unrecoverable (like bad auths).
func (s *SyncTask) syncOrRetry(src, dst *s3.Bucket, key s3.Key) (int, error) {
	var err error
	retry := 1
	for ; retry <= s.MaxRetry; retry++ {
		start := time.Now()

		// do a put copy call (sync directly from bucket to another
		// without fetching the content locally)
		metrics.syncAttempted.Add(1)
		metrics.inflight.Add(1)
		err = s.Sync(src, dst, key)
		metrics.inflight.Add(-1)

		metrics.secondsWaitingS3.Add(time.Since(start).Seconds())

		switch e := err.(type) {
		case nil:
			// when there are no errors, there's nothing to retry
			return retry, nil
		case *s3.Error:
			// if the error is specific to S3, we can do smart stuff like
			if s3.IsS3Error(e, s3.ErrNoSuchKey) {
				// when the key disappeared (occurs very often), don't retry
				// and quit right away. return no errors so the key is considered
				// sync'd (nothing to sync)
				return retry, nil
			}
			if shouldAbort(e) {
				// abort if its an error that will occur for all future calls
				// such as bad auth, or the bucket not existing anymore (that'd be bad!)
				logrus.WithFields(logrus.Fields{
					"key":        key,
					"s3_code":    e.Code,
					"s3_message": e.Message,
				}).Fatal("abort worthy error, should not continue to sync before issue is resolved")
			}
			if !shouldRetry(e) {
				// give up on that key if it's not retriable, such as a key
				// that was deleted
				logrus.WithFields(logrus.Fields{
					"key":        key,
					"s3_code":    e.Code,
					"s3_message": e.Message,
				}).Warn("unretriable error")
				return retry, e
			}
			// carry on to retry
		default:
			// carry on to retry
		}
		// log that we sleep, but don't log the error itself just
		// yet (to avoid logging transient network errors that are
		// recovered by retrying)
		metrics.syncRetries.Add(1)
		sleepFor := s.RetryBase * time.Duration(retry)
		logrus.WithFields(logrus.Fields{
			"sleep":     sleepFor,
			"retry":     retry,
			"max_retry": s.MaxRetry,
		}).Debug("sleeping on retryable error")
		time.Sleep(sleepFor)
	}
	return retry, err
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
	case s3.IsS3Error(err, s3.ErrAccountProblem):
	case s3.IsS3Error(err, s3.ErrCredentialsNotSupported):
	case s3.IsS3Error(err, s3.ErrInvalidAccessKeyID):
	case s3.IsS3Error(err, s3.ErrInvalidBucketName):
	case s3.IsS3Error(err, s3.ErrNoSuchBucket):
	}
	return true
}

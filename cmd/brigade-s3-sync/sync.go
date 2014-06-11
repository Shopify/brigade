package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"flag"
	"github.com/aybabtme/color/brush"
	"github.com/bmizerany/perks/quantile"
	"github.com/cheggaaa/pb"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"github.com/davecheney/profile"
	"github.com/dustin/go-humanize"
	"io"
	"log"
	"net/url"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"
)

var (
	MaxRetry = 5
	// One JSON decoder per core
	DecodePara = runtime.NumCPU()
	// Default concurrent sync calls to S3
	SyncPara = 200
	// Buffering of decode/sync channels,
	// which are BufferFactor-times bigger than their
	// parallelism.
	BufferFactor = 10

	P50 = 0.50
	P95 = 0.95
)

var (
	q    = struct{}{}
	elog *log.Logger

	// Stats to be manipulated using sync/atomic operations.
	filelines   int64
	decodedKeys int64
	syncedKeys  int64
	inflight    int64

	qtstreamL sync.Mutex
	qtstream  = quantile.NewTargeted(P50, P95)
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	w := tabwriter.NewWriter(os.Stdout, 16, 2, 2, ' ', 0)
	log.SetOutput(&lineTabWriter{w})
	log.SetFlags(log.Ltime)
	log.SetPrefix(brush.Blue("[info] ").String())
}

// pfatal is like elog.Fatalf but prints the flag defaults before exiting.
func pfatal(format string, args ...interface{}) {
	elog.Printf(format, args...)
	flag.PrintDefaults()
	os.Exit(2)
}

func main() {

	// write errors both to stderr and a file for late night readings
	efile, err := os.OpenFile(os.Args[0]+".elog", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0640)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = efile.Close() }()
	elog = log.New(io.MultiWriter(os.Stderr, efile),
		brush.Red("[error] ").String(),
		log.Ltime|log.Lshortfile)

	// flags and environment secrets
	var (
		// creds aren't passed as flag arguments as they would appear in the
		// process name.
		access     = os.Getenv("AWS_ACCESS_KEY")
		secret     = os.Getenv("AWS_SECRET_KEY")
		filename   = flag.String("filename", "", "name of the file containing the list of keys to sync")
		src        = flag.String("src", "", "src bucket to get the keys from")
		dst        = flag.String("dst", "", "dst bucket to put the keys into")
		para       = flag.Int("para", SyncPara, "number of parallel sync request")
		memprof    = flag.Bool("memprof", false, "track a memory profile")
		regionName = flag.String("aws-region", aws.USEast.Name, "AWS region")
	)
	flag.Parse()

	srcU, srcErr := url.Parse(*src)
	dstU, dstErr := url.Parse(*dst)
	SyncPara = *para

	region, validRegion := aws.Regions[*regionName]
	switch {
	case access == "":
		pfatal("needs an AWS access key\n")
	case secret == "":
		pfatal("needs an AWS secret key\n")
	case !validRegion:
		pfatal("%q is not a valid region name", *regionName)
	case *src == "":
		pfatal("need a source bucket to sync from")
	case srcErr != nil:
		pfatal("%q is not a valid source URL: %v", *src, srcErr)
	case *dst == "":
		pfatal("need a destination bucket to sync onto")
	case dstErr != nil:
		pfatal("%q is not a valid source URL: %v", *dst, dstErr)
	case *filename == "":
		pfatal("need a filename to read keys from")
	}

	auth := aws.Auth{AccessKey: access, SecretKey: secret}

	if *memprof {
		defer profile.Start(profile.MemProfile).Stop()
	}

	start := time.Now()

	ticker := time.NewTicker(time.Second)
	go printProgress(ticker)

	keys := make(chan s3.Key, SyncPara*BufferFactor)
	decoders := make(chan []byte, DecodePara*BufferFactor)

	// start JSON decoders
	log.Printf("starting %d key decoders, buffer size %d", DecodePara, cap(decoders))
	decGroup := sync.WaitGroup{}
	for i := 0; i < DecodePara; i++ {
		decGroup.Add(1)
		go decode(&decGroup, decoders, keys)
	}

	// start S3 sync workers
	log.Printf("starting %d key sync workers, buffer size %d", SyncPara, cap(keys))
	syncGroup := sync.WaitGroup{}
	for i := 0; i < SyncPara; i++ {
		syncGroup.Add(1)
		go syncKey(&syncGroup, s3.New(auth, region), srcU, dstU, keys)
	}

	// feed the pipeline by reading the source file
	log.Printf("reading keys to sync from %q", *filename)
	file, err := os.Open(*filename)
	if err != nil {
		pfatal("couldn't open %q: %v", *src, err)
	}
	defer func() { _ = file.Close() }()

	log.Printf("starting to read key file")
	readLines(file, decoders)

	// when done reading the source file, wait until the decoders
	// are done.
	log.Printf("done reading %s lines from %q in %v",
		humanize.Comma(atomic.LoadInt64(&filelines)),
		*filename,
		time.Since(start))
	close(decoders)
	decGroup.Wait()

	// when the decoders are all done, wait for the sync workers to finish
	log.Printf("done decoding %s keys in %v",
		humanize.Comma(atomic.LoadInt64(&decodedKeys)),
		time.Since(start))

	close(keys)
	syncGroup.Wait()
	ticker.Stop()

	// the source file is read, all keys were decoded and sync'd. we're done.
	log.Printf("done syncing %s keys in %v",
		humanize.Comma(atomic.LoadInt64(&syncedKeys)),
		time.Since(start))
}

// prints progress and stats as we go, handy to figure out what's going on
// and how the tool performs.
func printProgress(tick *time.Ticker) {
	for _ = range tick.C {
		qtstreamL.Lock()
		p50, p95 := qtstream.Query(P50), qtstream.Query(P95)
		qtstream.Reset()
		qtstreamL.Unlock()

		log.Printf("filelines=%s\tdecodedKeys=%s\tsyncedKeys=%s\tinflight=%d/%d\tsync-p50=%v\tsync-p95=%v",
			humanize.Comma(filelines),
			humanize.Comma(decodedKeys),
			humanize.Comma(syncedKeys),
			atomic.LoadInt64(&inflight), SyncPara,
			time.Duration(p50),
			time.Duration(p95),
		)
	}
}

// reads all the \n separated lines from a file, printing progress with a
// fancy progress bar.
func readLines(file *os.File, decoders chan<- []byte) {
	fi, err := file.Stat()
	if err != nil {
		pfatal("couldn't state %q: %v", file.Name(), err)
	}
	// tracking the progress in reading the file helps tracking
	// how far in the sync process we are.
	bar := pb.New(int(fi.Size()))
	bar.ShowBar = true
	bar.ShowCounters = true
	bar.ShowPercent = true
	bar.ShowSpeed = true
	bar.ShowTimeLeft = true
	bar.SetUnits(pb.U_BYTES)
	barr := bar.NewProxyReader(file)

	gr, err := gzip.NewReader(barr)
	if err != nil {
		pfatal("couldn't open gzip stream on file %q: %v", file.Name(), err)
	}
	defer func() { _ = gr.Close() }()

	rd := bufio.NewReader(gr)

	bar.Start()
	defer bar.Finish()
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
		atomic.AddInt64(&filelines, 1)
	}
}

// decodes s3.Keys from a channel of bytes, each byte containing a full key
func decode(wg *sync.WaitGroup, lines <-chan []byte, keys chan<- s3.Key) {
	defer wg.Done()
	var key s3.Key
	for line := range lines {
		err := json.Unmarshal(line, &key)
		if err != nil {
			elog.Printf("unmarshaling line: %v", err)
		} else {
			keys <- key
			atomic.AddInt64(&decodedKeys, 1)
		}
	}
}

func syncKey(wg *sync.WaitGroup, sss *s3.S3, src, dst *url.URL, keys <-chan s3.Key) {
	defer wg.Done()

	srcBkt := sss.Bucket(src.Host)
	dstBkt := sss.Bucket(dst.Host)
	// before starting the sync, make sure our s3 object is usable (credentials and such)
	_, err := dstBkt.List("/", "/", "/", 1)
	if err != nil {
		// if we can't list, we abort right away
		elog.Fatalf("couldn't list destination bucket %q: %v", dst, err)
	}

	opts := s3.CopyOptions{}

	for key := range keys {
		retry := 1
	retrying:
		for ; retry <= MaxRetry; retry++ {
			start := time.Now()

			srcPath := srcBkt.Name + "/" + key.Key

			// do a put copy call (sync directly from bucket to another
			// without fetching the content locally)
			atomic.AddInt64(&inflight, 1)
			_, err = dstBkt.PutCopy(key.Key, s3.Private, opts, srcPath)
			atomic.AddInt64(&inflight, -1)
			qtstreamL.Lock()
			qtstream.Insert(float64(time.Since(start).Nanoseconds()))
			qtstreamL.Unlock()

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
			// yet (to avoid logging transient network erros that are
			// recovered by retrying)
			sleepFor := time.Second * time.Duration(retry)
			elog.Printf("worker-sleep-on-retryiable-error=%v", sleepFor)
			time.Sleep(sleepFor)
			elog.Printf("worker-wake-up, retries=%d/%d", retry, MaxRetry)

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

		atomic.AddInt64(&syncedKeys, 1)
	}
}

// Pretty printed logs
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

// Classify S3 errors that should be retried.
func shouldRetry(err *s3.Error) bool {
	switch err.Code {
	case ErrExpiredToken,
		ErrIncompleteBody,
		ErrInternalError,
		ErrInvalidBucketState,
		ErrInvalidObjectState,
		ErrInvalidPart,
		ErrInvalidPartOrder,
		ErrOperationAborted,
		ErrPermanentRedirect,
		ErrPreconditionFailed,
		ErrRedirect,
		ErrRequestTimeout,
		ErrRequestTimeTooSkewed,
		ErrServiceUnavailable,
		ErrTemporaryRedirect,
		ErrTokenRefreshRequired,
		ErrUnexpectedContent,
		ErrSlowDown:
		return true
	}
	return false
}

// Classify S3 errors that require aborting the whole sync process.
func shouldAbort(err *s3.Error) bool {
	switch err.Code {
	case ErrAccessDenied,
		ErrAccountProblem,
		ErrCredentialsNotSupported,
		ErrInvalidAccessKeyId,
		ErrInvalidBucketName,
		ErrNoSuchBucket:
		return true
	}
	return false
}

// List of all AWS S3 error codes, extracted from:
//    http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
const (
	ErrAccessDenied                            = "AccessDenied"
	ErrAccountProblem                          = "AccountProblem"
	ErrAmbiguousGrantByEmailAddress            = "AmbiguousGrantByEmailAddress"
	ErrBadDigest                               = "BadDigest"
	ErrBucketAlreadyExists                     = "BucketAlreadyExists"
	ErrBucketAlreadyOwnedByYou                 = "BucketAlreadyOwnedByYou"
	ErrBucketNotEmpty                          = "BucketNotEmpty"
	ErrCredentialsNotSupported                 = "CredentialsNotSupported"
	ErrCrossLocationLoggingProhibited          = "CrossLocationLoggingProhibited"
	ErrEntityTooSmall                          = "EntityTooSmall"
	ErrEntityTooLarge                          = "EntityTooLarge"
	ErrExpiredToken                            = "ExpiredToken"
	ErrIllegalVersioningConfigurationException = "IllegalVersioningConfigurationException"
	ErrIncompleteBody                          = "IncompleteBody"
	ErrIncorrectNumberOfFilesInPostRequest     = "IncorrectNumberOfFilesInPostRequest"
	ErrInlineDataTooLarge                      = "InlineDataTooLarge"
	ErrInternalError                           = "InternalError"
	ErrInvalidAccessKeyId                      = "InvalidAccessKeyId"
	ErrInvalidAddressingHeader                 = "InvalidAddressingHeader"
	ErrInvalidArgument                         = "InvalidArgument"
	ErrInvalidBucketName                       = "InvalidBucketName"
	ErrInvalidBucketState                      = "InvalidBucketState"
	ErrInvalidDigest                           = "InvalidDigest"
	ErrInvalidLocationConstraint               = "InvalidLocationConstraint"
	ErrInvalidObjectState                      = "InvalidObjectState"
	ErrInvalidPart                             = "InvalidPart"
	ErrInvalidPartOrder                        = "InvalidPartOrder"
	ErrInvalidPayer                            = "InvalidPayer"
	ErrInvalidPolicyDocument                   = "InvalidPolicyDocument"
	ErrInvalidRange                            = "InvalidRange"
	ErrInvalidRequest                          = "InvalidRequest"
	ErrInvalidSecurity                         = "InvalidSecurity"
	ErrInvalidSOAPRequest                      = "InvalidSOAPRequest"
	ErrInvalidStorageClass                     = "InvalidStorageClass"
	ErrInvalidTargetBucketForLogging           = "InvalidTargetBucketForLogging"
	ErrInvalidToken                            = "InvalidToken"
	ErrInvalidURI                              = "InvalidURI"
	ErrKeyTooLong                              = "KeyTooLong"
	ErrMalformedACLError                       = "MalformedACLError"
	ErrMalformedPOSTRequest                    = "MalformedPOSTRequest"
	ErrMalformedXML                            = "MalformedXML"
	ErrMaxMessageLengthExceeded                = "MaxMessageLengthExceeded"
	ErrMaxPostPreDataLengthExceededError       = "MaxPostPreDataLengthExceededError"
	ErrMetadataTooLarge                        = "MetadataTooLarge"
	ErrMethodNotAllowed                        = "MethodNotAllowed"
	ErrMissingAttachment                       = "MissingAttachment"
	ErrMissingContentLength                    = "MissingContentLength"
	ErrMissingRequestBodyError                 = "MissingRequestBodyError"
	ErrMissingSecurityElement                  = "MissingSecurityElement"
	ErrMissingSecurityHeader                   = "MissingSecurityHeader"
	ErrNoLoggingStatusForKey                   = "NoLoggingStatusForKey"
	ErrNoSuchBucket                            = "NoSuchBucket"
	ErrNoSuchKey                               = "NoSuchKey"
	ErrNoSuchLifecycleConfiguration            = "NoSuchLifecycleConfiguration"
	ErrNoSuchUpload                            = "NoSuchUpload"
	ErrNoSuchVersion                           = "NoSuchVersion"
	ErrNotImplemented                          = "NotImplemented"
	ErrNotSignedUp                             = "NotSignedUp"
	ErrNotSuchBucketPolicy                     = "NotSuchBucketPolicy"
	ErrOperationAborted                        = "OperationAborted"
	ErrPermanentRedirect                       = "PermanentRedirect"
	ErrPreconditionFailed                      = "PreconditionFailed"
	ErrRedirect                                = "Redirect"
	ErrRestoreAlreadyInProgress                = "RestoreAlreadyInProgress"
	ErrRequestIsNotMultiPartContent            = "RequestIsNotMultiPartContent"
	ErrRequestTimeout                          = "RequestTimeout"
	ErrRequestTimeTooSkewed                    = "RequestTimeTooSkewed"
	ErrRequestTorrentOfBucketError             = "RequestTorrentOfBucketError"
	ErrSignatureDoesNotMatch                   = "SignatureDoesNotMatch"
	ErrServiceUnavailable                      = "ServiceUnavailable"
	ErrSlowDown                                = "SlowDown"
	ErrTemporaryRedirect                       = "TemporaryRedirect"
	ErrTokenRefreshRequired                    = "TokenRefreshRequired"
	ErrTooManyBuckets                          = "TooManyBuckets"
	ErrUnexpectedContent                       = "UnexpectedContent"
	ErrUnresolvableGrantByEmailAddress         = "UnresolvableGrantByEmailAddress"
	ErrUserKeyMustBeSpecified                  = "UserKeyMustBeSpecified"
)

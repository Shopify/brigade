package backup

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"github.com/Shopify/brigade/cmd/diff"
	"github.com/Shopify/brigade/cmd/list"
	cmdsync "github.com/Shopify/brigade/cmd/sync"
	"github.com/Sirupsen/logrus"
	"github.com/aybabtme/goamz/s3"
	"github.com/dustin/go-humanize"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

const (
	megabyte int64 = 1 << 20
	maxRetry       = 5

	timeFormat = time.RFC3339
	sourceSfx  = "_source_list.json.gz"
	diffSfx    = "_diff_list.json.gz"
	okSfx      = "_ok_list.json.gz"
	failSfx    = "_fail_list.json.gz"
)

func toSourceName(t time.Time) string { return toSfxName(t, sourceSfx) }
func toDiffName(t time.Time) string   { return toSfxName(t, diffSfx) }
func toOkName(t time.Time) string     { return toSfxName(t, okSfx) }
func toFailName(t time.Time) string   { return toSfxName(t, failSfx) }

func fromSourceName(name string) (time.Time, bool, error) { return fromSfxName(name, sourceSfx) }
func fromDiffName(name string) (time.Time, bool, error)   { return fromSfxName(name, diffSfx) }
func fromOkName(name string) (time.Time, bool, error)     { return fromSfxName(name, okSfx) }
func fromFailName(name string) (time.Time, bool, error)   { return fromSfxName(name, failSfx) }

func toSfxName(t time.Time, sfx string) string {
	return t.Format(timeFormat) + sfx
}

func fromSfxName(name, sfx string) (time.Time, bool, error) {
	idx := strings.Index(name, sfx)
	if idx < 0 || idx > len(name) {
		return time.Time{}, false, nil
	}
	t, err := time.Parse(timeFormat, name[:idx])
	return t, err == nil, err
}

// Backup a bucket to another in batch, first listing the source bucket,
// comparing the listing to a prior one and synching the keys that differ.
type Backup struct {
	timestamp time.Time

	src   *s3.Bucket
	dst   *s3.Bucket
	state *s3.Bucket

	srcPath   string
	statePath string

	listFile *os.File
	diffFile *os.File
	okFile   *os.File
	failFile *os.File
}

// Cleanup all the files used by the backup. Returns the first error, logs them all.
func (b *Backup) Cleanup(deleteFiles bool) error {
	var cerr error
	for _, file := range []*os.File{b.listFile, b.diffFile, b.okFile, b.failFile} {
		if file == nil {
			continue
		}
		localLog := logrus.WithField("filename", file.Name())
		if fi, err := file.Stat(); err == nil {
			localLog = localLog.WithField("size", fi.Size())
		}
		if err := file.Close(); err != nil {
			localLog.WithField("error", err).Error("closing file")
			if cerr == nil {
				cerr = err
			}
		}
		if deleteFiles {
			err := os.Remove(file.Name())
			if err != nil {
				localLog.WithField("error", err).Error("removing file")
				if cerr == nil {
					cerr = err
				}
			}
			localLog.Info("file removed")
		} else {
			localLog.Info("file left intact")
		}
	}
	return cerr
}

// NewBackup creates a backup task and the files it will use to perform the
// backup of a bucket.
func NewBackup(src, dst, state *s3.Bucket, srcPath, statePath string) (*Backup, error) {
	start := time.Now()

	listFile, err := os.Create(toSourceName(start))
	if err != nil {
		return nil, fmt.Errorf("couldn't create source list: %v", err)
	}

	diffFile, err := os.Create(toDiffName(start))
	if err != nil {
		return nil, fmt.Errorf("couldn't create diff list: %v", err)
	}

	okFile, err := os.Create(toOkName(start))
	if err != nil {
		return nil, fmt.Errorf("couldn't create ok list: %v", err)
	}

	failFile, err := os.Create(toFailName(start))
	if err != nil {
		return nil, fmt.Errorf("couldn't create fail list: %v", err)
	}

	return &Backup{
		timestamp: start,
		src:       src,
		dst:       dst,
		state:     state,

		srcPath:   srcPath,
		statePath: statePath,

		listFile: listFile,
		diffFile: diffFile,
		okFile:   okFile,
		failFile: failFile,
	}, nil
}

func readyForRead(f *os.File) error {
	err := f.Sync()
	if err != nil {
		return fmt.Errorf("syncing file: %v", err)
	}
	_, err = f.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("seeking to beginning of file: %v", err)
	}
	return nil
}

// Execute starts the backup by listing, diffing, syncing and then
// persisting the result to S3.
func (b *Backup) Execute() error {
	logrus.Info("starting backup execution")

	// list
	logrus.Info("[1/4] listing source bucket")
	listWr := gzip.NewWriter(b.listFile)

	err := list.List(b.src.S3, b.src.Name, b.srcPath, listWr)
	if err != nil {
		return fmt.Errorf("listing bucket %q: %v", b.src.Name, err)
	}
	if err := listWr.Close(); err != nil {
		return fmt.Errorf("closing gzip writer on bucket listing: %v", err)
	}

	if err := readyForRead(b.listFile); err != nil {
		return fmt.Errorf("readying listing file %q: %v", b.listFile.Name(), err)
	}

	// preprocess
	logrus.Info("[2/4] preprocessing bucket list")
	logrus.Info("[2/4] - fetching most recent list")
	lastList, err := ioutil.TempFile("", "last_source_listing")
	if err != nil {
		return fmt.Errorf("creating temp file for last listing: %v", err)
	}
	defer func() {
		_ = lastList.Close()
		_ = os.Remove(lastList.Name())
	}()

	found, err := findLastList(b.state, b.statePath, lastList)
	if err != nil {
		return fmt.Errorf("finding last listing: %v", err)
	}
	if err := lastList.Sync(); err != nil {
		return fmt.Errorf("syncing last listing to disk: %v", err)
	}

	var src *os.File
	if found {
		logrus.Info("[2/4] - computing difference between old list and this one")
		err := computeSourceList(lastList, b.listFile, b.diffFile)
		if err != nil {
			logrus.WithField("error", err).Warn("error during diff, proceeding with full listing sync")
			src = b.listFile
		} else {
			src = b.diffFile
		}
		_ = lastList.Close()
	} else {
		logrus.Info("[2/4] - nothing found, doing backup of full listing")
		src = b.listFile
	}
	if err := readyForRead(src); err != nil {
		return fmt.Errorf("readying source list file %q: %v", src.Name(), err)
	}

	// sync
	logrus.Info("[3/4] syncing source to destination bucket")
	if err := performSync(b.src, b.dst, src, b.okFile, b.failFile); err != nil {
		return fmt.Errorf("performing sync: %v", err)
	}

	// Persist artifacts
	logrus.Info("[4/4] persisting backup artifacts")
	err = b.persist()
	if err != nil {
		return fmt.Errorf("persisting result of backup: %v", err)
	}
	logrus.Info("backup completed without error")
	return nil
}

func computeSourceList(lastList, newList io.Reader, diffList io.Writer) error {
	lastListGzr, err := gzip.NewReader(lastList)
	if err != nil {
		return fmt.Errorf("no gzip stream from last list: %v", err)
	}
	defer func() { _ = lastListGzr.Close() }()

	listGzr, err := gzip.NewReader(newList)
	if err != nil {
		return fmt.Errorf("no gzip stream from bucket list: %v", err)
	}
	defer func() { _ = listGzr.Close() }()

	diffGzw := gzip.NewWriter(diffList)
	defer func() {
		if err = diffGzw.Close(); err != nil {
			logrus.WithField("error", err).Warn("error closing gzip writer of diff, proceeding with full listing sync")
		}
	}()

	return diff.Diff(lastListGzr, listGzr, diffGzw)
}

func performSync(src, dst *s3.Bucket, list io.Reader, ok, fail io.Writer) error {
	syncTask, err := cmdsync.NewSyncTask(src, dst)
	if err != nil {
		return fmt.Errorf("preparing sync task: %v", err)
	}

	srcGr, err := gzip.NewReader(list)
	if err != nil {
		return fmt.Errorf("getting reader from source listing: %v", err)
	}
	defer func() { _ = srcGr.Close() }()

	okGzw := gzip.NewWriter(ok)
	failGzw := gzip.NewWriter(fail)
	defer func() {
		if err := okGzw.Close(); err != nil {
			logrus.WithField("error", err).Error("closing gzip writer on ok file")
		}
		if err := failGzw.Close(); err != nil {
			logrus.WithField("error", err).Error("closing gzip writer on fail file")
		}
	}()
	return syncTask.Start(srcGr, okGzw, failGzw)
}

func findLastList(bkt *s3.Bucket, pfx string, dest io.WriteSeeker) (bool, error) {
	res, err := bkt.List(pfx, "/", "", 10000)
	if err != nil {
		return false, fmt.Errorf("listing %q: %v", pfx, err)
	}
	logrus.WithField("name", res.Name).Info("fetching previous listings")
	var (
		mostRecentTime time.Time
		mostRecentKey  s3.Key
	)

	for _, key := range res.Contents {
		basename := path.Base(key.Key)
		logrus.WithFields(logrus.Fields{
			"basename": basename,
			"key":      key.Key,
		}).Warn("looking at key")

		t, found, err := fromSourceName(basename)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"error": err,
				"key":   key.Key,
			}).Warn("malformed source list name")
		}
		if !found {
			continue
		}

		if t.After(mostRecentTime) {
			mostRecentTime = t
			mostRecentKey = key
		}
	}

	if mostRecentTime.IsZero() {
		// we havent found a most recent source backup
		return false, nil
	}
	logrus.WithField("key", mostRecentKey.Key).Info("found most recent artifact")
	// retry maxRetry times
	var rd io.Reader
	for i := 0; i < maxRetry; i++ {
		rd, err = bkt.GetReader(mostRecentKey.Key)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"error": err,
				"key":   mostRecentKey.Key,
			}).Error("fetching most recent key, sleeping")
			time.Sleep(time.Second)
			continue
		}
		bufr := bufio.NewReader(rd)
		_, err = dest.Seek(0, 0)
		if err != nil {
			return false, fmt.Errorf("seeking to begining of destination: %v", err)
		}
		_, err = io.Copy(dest, bufr)
		if err == nil {
			_, err = dest.Seek(0, 0)
			return true, err
		}
	}
	logrus.WithFields(logrus.Fields{
		"error":    err,
		"key":      mostRecentKey.Key,
		"maxRetry": maxRetry,
	}).Error("failed too many times")
	return false, err
}

func (b *Backup) persist() error {

	files := []*os.File{b.listFile, b.okFile, b.failFile}
	errc := make(chan error, len(files))
	wg := sync.WaitGroup{}
	for _, file := range files {
		wg.Add(1)
		go func(w *sync.WaitGroup, f *os.File) {
			defer w.Done()
			if err := b.doPersist(f); err != nil {
				logrus.WithFields(logrus.Fields{
					"error": err,
					"file":  f.Name(),
				}).Error("failed to persist backup artifact")
				errc <- err
			} else {
				logrus.WithFields(logrus.Fields{
					"file": f.Name(),
				}).Info("done persisting artifact")
			}
		}(&wg, file)
	}
	wg.Wait()
	close(errc)
	logrus.Info("done persisting artifact")
	return <-errc
}

func (b *Backup) doPersist(f *os.File) error {
	err := readyForRead(f)
	if err != nil {
		return fmt.Errorf("readying for read: %v", err)
	}
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	dstName := path.Join(b.statePath, f.Name())
	localLog := logrus.WithFields(logrus.Fields{
		"file":        f.Name(),
		"destination": dstName,
	})
	localLog.Info("sending file to S3")

	for i := 0; i < maxRetry; i++ {
		err = b.state.PutReader(dstName, f, fi.Size(), "", s3.BucketOwnerFull, s3.Options{})
		if s3.IsS3Error(err, s3.ErrEntityTooLarge) {
			localLog.Info("file too large, doing multipart upload")
			return multipartPut(b.state, dstName, f, fi)
		}
		if err == nil {
			break
		}
	}
	return err
}

func multipartPut(bkt *s3.Bucket, keyname string, file *os.File, fi os.FileInfo) error {
	var err error
	for i := 0; i < maxRetry; i++ {
		err = doMultipartPut(bkt, keyname, file, fi)
		if err == nil {
			return err
		}
		logrus.WithFields(logrus.Fields{
			"filename": file.Name(),
			"size":     fi.Size(),
			"attempt":  i,
			"error":    err,
		}).Error("failed an attempt to do multipart put")
	}
	logrus.WithFields(logrus.Fields{
		"filename": file.Name(),
		"size":     fi.Size(),
		"error":    err,
	}).Error("failed too many times to do multipart put")
	return err
}

func doMultipartPut(bkt *s3.Bucket, keyname string, file *os.File, fi os.FileInfo) error {
	partSize := 100 * megabyte
	localLog := logrus.WithFields(logrus.Fields{
		"filename": file.Name(),
		"size":     fi.Size(),
		"partSize": partSize,
	})
	localLog.Info("initializing multipart upload")

	multi, err := bkt.InitMulti(keyname, "", s3.BucketOwnerFull)
	if err != nil {
		return fmt.Errorf("initializing multipart upload: %v", err)
	}

	localLog.Info("starting multipart upload")
	parts, err := multi.PutAll(file, partSize)
	if err != nil {
		_ = multi.Abort()
		return fmt.Errorf("putting all %s parts: %v", humanize.Bytes(uint64(partSize)), err)
	}

	localLog.WithField("parts", len(parts)).Info("completing multipart upload")
	if err := multi.Complete(parts); err != nil {
		_ = multi.Abort()
		return fmt.Errorf("completing multipart upload: %v", err)
	}
	localLog.WithField("parts", len(parts)).Info("multipart upload successful")
	return nil
}

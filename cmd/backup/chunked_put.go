package backup

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/aybabtme/goamz/s3"
	"github.com/aybabtme/humanize"
)

func multipartPut(bkt *s3.Bucket, keyname string, src s3.ReaderAtSeeker, size int64) error {
	var err error
	for i := 0; i < maxRetry; i++ {
		_, err = src.Seek(0, 0)
		if err != nil {
			return err
		}
		err = doMultipartPut(bkt, keyname, src, size)
		if err == nil {
			return nil
		}
		log.WithFields(log.Fields{
			"size":    size,
			"attempt": i,
			"error":   err,
		}).Error("failed an attempt to do multipart put")
	}
	log.WithFields(log.Fields{
		"size":  size,
		"error": err,
	}).Error("failed too many times to do multipart put")
	return err
}

func doMultipartPut(bkt *s3.Bucket, keyname string, src s3.ReaderAtSeeker, size int64) error {
	partSize := int64(10 * megabyte)
	localLog := log.WithFields(log.Fields{
		"size":     size,
		"partSize": partSize,
	})
	localLog.Info("initializing multipart upload")

	multi, err := bkt.InitMulti(keyname, "", s3.BucketOwnerFull)
	if err != nil {
		return fmt.Errorf("initializing multipart upload: %v", err)
	}

	localLog.Info("starting multipart upload")
	parts, err := multi.PutAll(src, partSize)
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

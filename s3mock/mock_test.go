package s3mock_test

import (
	"bytes"
	"github.com/Shopify/brigade/s3mock"
	"github.com/Shopify/brigade/s3util"
	"testing"
)

func TestPerfMockBucketKnownKey(t *testing.T) {
	mock := s3mock.NewMock(t).Seed(s3mock.NewPerfBucket(t))
	defer mock.Close()

	bkt := mock.S3().Bucket("shopify-perf")

	data, err := bkt.Get("ruby-2.1.1-webscale1.tar.gz")
	if err != nil {
		t.Errorf("should have found file: %v", err)
	}

	if bytes.Equal(data, nil) {
		t.Errorf("should have returned data, got %q", data)
	}
}

func TestPerfMockBucketUnknownKey(t *testing.T) {
	mock := s3mock.NewMock(t).Seed(s3mock.NewPerfBucket(t))
	defer mock.Close()

	bkt := mock.S3().Bucket("shopify-perf")

	data, err := bkt.Get("not a key in this bucket")

	if !s3util.IsS3Error(err, s3util.ErrNoSuchKey) {
		t.Errorf("expected 'no such key' error, got %v", err)
	}

	if !bytes.Equal(data, nil) {
		t.Errorf("should have returned nil data, got %q", data)
	}
}

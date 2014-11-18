package s3mock

import (
	"encoding/json"
	"github.com/aybabtme/goamz/aws"
	"github.com/aybabtme/goamz/s3"
	"github.com/aybabtme/goamz/s3/s3test"
	"github.com/dustin/randbo"
	"io"
	"testing"
)

// MockS3 is a helper to setup a fake S3 that has prepopulated buckets.
type MockS3 struct {
	t      *testing.T
	fakes3 *s3.S3
	srv    *s3test.Server
}

// NewMock creates an S3 mock that fails tests if it errors.
func NewMock(t *testing.T) *MockS3 {
	srv, err := s3test.NewServer(&s3test.Config{})
	if err != nil {
		t.Fatalf("s3mock.NewMock: couldn't create test s3 server, %v", err)
	}
	region := aws.Region{
		Name:                 "faux-region-1",
		S3Endpoint:           srv.URL(),
		S3LocationConstraint: true,
	}
	return &MockS3{
		t:      t,
		fakes3: s3.New(aws.Auth{}, region),
		srv:    srv,
	}
}

// S3 is the fake S3 object to use to interact with this mock S3.
func (m *MockS3) S3() *s3.S3 { return m.fakes3 }

// Close the mock resources.
func (m *MockS3) Close() { m.srv.Quit() }

// ListBuckets gives a snapshot of the buckets on S3.
func (m *MockS3) ListBuckets() map[string]s3test.Bucket {
	return m.srv.Buckets()
}

// Seed adds all the keys in mockBkt to the fake s3 server, sending nil data
func (m *MockS3) Seed(mockBkt MockBucket) *MockS3 {

	bkt := m.S3().Bucket(mockBkt.Name())
	if err := bkt.PutBucket(s3.Private); err != nil {
		m.t.Fatalf("s3mock.MockS3.Seed: couldn't create bucket: %v", err)
		return m
	}

	rand := randbo.New()
	data := make([]byte, 100)

	for _, key := range mockBkt.Keys() {
		_, _ = rand.Read(data)
		err := bkt.Put(key.Key, data, "", s3.Private, s3.Options{})
		if err != nil {
			m.t.Fatalf("s3mock.MockS3.Seed: couldn't create key %q: %v", key.Key, err)
		}
	}
	return m
}

// SendErrors will start sending errors at a rate from 0.0 to 1.0,
// where 1.0 means always send an error. The errors will start after
// enough error-free calls have gone through.
func (m *MockS3) SendErrors(afterCalls int, rate float64, errs []s3.Error) {
	m.srv.SendErrors(afterCalls, rate, errs)
}

// MockBucket contains keys that are prepopulated
type MockBucket struct {
	name string
	keys []s3.Key
}

// Name of the bucket
func (m *MockBucket) Name() string { return m.name }

// Keys in the bucket
func (m *MockBucket) Keys() []s3.Key { return m.keys }

// NewPerfBucket creates a fake S3 bucket populated with the keys in the Shopify
// Perf bucket. This bucket is used because its keys contains no confidential data.
// See file bucket_list.json.
func NewPerfBucket(t *testing.T) MockBucket {

	bucketname := "shopify-perf"
	perffilename := "bucket_list.json"

	perfKeys, ok := GetBucketListJSON(perffilename)
	if !ok {
		t.Fatalf("s3mock.NewPerfBucket: expected content in %q, got nothing", perffilename)
	}

	var key s3.Key
	var keys []s3.Key
	dec := json.NewDecoder(perfKeys)

decoding:
	for {
		err := dec.Decode(&key)
		switch err {
		case io.EOF:
			break decoding
		case nil:
			keys = append(keys, key)
		default:
			t.Fatalf("s3mock.NewPerfBucket: decoding %q: %v", perffilename, err)
		}
	}

	return MockBucket{name: bucketname, keys: keys}
}

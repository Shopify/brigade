package s3mock

import (
	"encoding/json"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"github.com/crowdmob/goamz/s3/s3test"
	"github.com/dustin/randbo"
	"io"
	"testing"
)

// MockBucket contains keys that are prepopulated
type MockBucket interface {
	// Name of the bucket
	Name() string
	// Keys in the bucket
	Keys() []s3.Key
}

// MockS3 is a helper to setup a fake S3 that has prepopulated buckets.
type MockS3 interface {
	// S3 is the fake S3 object to use to interact with this mock S3.
	S3() *s3.S3
	// Seed the mock bucket into this mock S3.
	Seed(MockBucket) MockS3
	// Close the mock resources.
	Close()
}

type mockS3 struct {
	t      *testing.T
	fakes3 *s3.S3
	srv    *s3test.Server
}

// NewMock creates an S3 mock that fails tests if it errors.
func NewMock(t *testing.T) MockS3 {
	srv, err := s3test.NewServer(&s3test.Config{})
	if err != nil {
		t.Fatalf("s3mock.NewMock: couldn't create test s3 server, %v", err)
	}
	region := aws.Region{
		Name:                 "faux-region-1",
		S3Endpoint:           srv.URL(),
		S3LocationConstraint: true,
	}
	return &mockS3{
		t:      t,
		fakes3: s3.New(aws.Auth{}, region),
		srv:    srv,
	}
}

func (m *mockS3) S3() *s3.S3 { return m.fakes3 }
func (m *mockS3) Close()     { m.srv.Quit() }

// adds all the keys in mockBkt to the fake s3 server, sending nil data
func (m *mockS3) Seed(mockBkt MockBucket) MockS3 {

	bkt := m.S3().Bucket(mockBkt.Name())
	if err := bkt.PutBucket(s3.Private); err != nil {
		m.t.Fatalf("s3mock.mockS3.Seed: couldn't create bucket: %v", err)
		return m
	}

	rand := randbo.NewFast()
	data := make([]byte, 100)

	for _, key := range mockBkt.Keys() {
		_, _ = rand.Read(data)
		err := bkt.Put(key.Key, data, "", s3.Private, s3.Options{})
		if err != nil {
			m.t.Fatalf("s3mock.mockS3.Seed: couldn't create key %q: %v", key.Key, err)
		}
	}
	return m
}

// skeleton for a mock bucket
type mockBkt struct {
	name string
	keys []s3.Key
}

func (m *mockBkt) Name() string   { return m.name }
func (m *mockBkt) Keys() []s3.Key { return m.keys }

// NewPerfBucket creates a fake S3 bucket populated with the keys in the Shopify
// Perf bucket. This bucket is used because its keys contains no confidential data.
// See file bucket_list.json.
func NewPerfBucket(t *testing.T) MockBucket {

	bucketname := "shopify-perf"
	perffilename := "bucket_list.json"

	perfKeys, ok := GetBucketListJson(perffilename)
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

	return &mockBkt{name: bucketname, keys: keys}
}

package sync_test

import (
	"bytes"
	"encoding/json"
	"github.com/Shopify/brigade/cmd/sync"
	"github.com/Shopify/brigade/s3mock"
	"github.com/aybabtme/goamz/s3"
	"github.com/kr/pretty"
	"io"
	"log"
	"os"
	"sort"
	"testing"
	"time"
)

func TestCanSync(t *testing.T) {
	time.AfterFunc(time.Second*10, func() { panic("infinite loop?") })
	// log.SetOutput(testwriter(t))

	mockbkt := s3mock.NewPerfBucket(t)
	mocks3 := s3mock.NewMock(t).Seed(mockbkt)
	defer mocks3.Close()

	dstname := "dst-bucket"
	src := mocks3.S3().Bucket(mockbkt.Name())
	dst := mocks3.S3().Bucket(dstname)
	dst.PutBucket(s3.Private) // create it

	input := encodeKeys(mockbkt.Keys())

	sync.Sync(testlogger(t), input, src, dst, sync.Options{
		DecodePara: 3,
		SyncPara:   3,
		RetryBase:  time.Millisecond,
	})

	want, ok := mocks3.ListBuckets()[mockbkt.Name()]
	if !ok {
		t.Fatalf("source bkt not found")
	}
	got, ok := mocks3.ListBuckets()[dstname]
	if !ok {
		t.Fatalf("destination bkt not found")
	}

	if len(want.Objects) != len(got.Objects) {
		t.Fatalf("want %d object, got %d", len(want.Objects), len(got.Objects))
	}

	for name, wantobj := range want.Objects {
		gotobj, ok := got.Objects[name]
		if !ok {
			t.Errorf("not found, but want obj %# v", pretty.Formatter(wantobj))
			continue
		}

		if bytes.Compare(wantobj.Data, gotobj.Data) != 0 {
			t.Logf("want.Data=%v", wantobj.Data)
			t.Logf(" got.Data=%v", gotobj.Data)
			t.Error("want some data, got something else")
		}
	}
}

// helpers

// io.Writer implementer
type writer func(p []byte) (int, error)

func (w writer) Write(p []byte) (int, error) { return w(p) }

// magic, a testing.T writer!
func testwriter(t *testing.T) io.Writer {
	return writer(func(p []byte) (int, error) {
		t.Log(string(p))
		return 0, nil
	})
}

// magic, a testing.T logger!
func testlogger(t *testing.T) *log.Logger {
	return log.New(io.MultiWriter(os.Stderr, testwriter(t)), "[test] ", 0)
}

// encode s3 keys from a json writer, fatals on error
func encodeKeys(keys []s3.Key) *bytes.Buffer {
	out := bytes.NewBuffer(nil)
	enc := json.NewEncoder(out)
	for _, key := range keys {
		err := enc.Encode(&key)
		if err != nil {
			log.Fatalf("encoding buf, %v", err)
		}
	}
	return out
}

// sort s3 keys lexicographically, by etag
func sortKeys(orig []s3.Key) []s3.Key {
	keys := make([]s3.Key, len(orig))
	copy(keys, orig)
	ks := keyslice(keys)
	sort.Sort(&ks)
	return keys
}

type keyslice []s3.Key

func (k keyslice) Len() int       { return len(k) }
func (k *keyslice) Swap(i, j int) { (*k)[i], (*k)[j] = (*k)[j], (*k)[i] }
func (k keyslice) Less(i, j int) bool {
	return bytes.Compare([]byte(k[i].ETag), []byte(k[j].ETag)) == -1
}

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
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"
)

func TestCanSync(t *testing.T) {
	time.AfterFunc(time.Second*10, func() { panic("infinite loop?") })
	log.SetOutput(testwriter(t))

	mockbkt := s3mock.NewPerfBucket(t)
	mocks3 := s3mock.NewMock(t).Seed(mockbkt)
	defer mocks3.Close()

	dstname := "dst-bucket"
	src := mocks3.S3().Bucket(mockbkt.Name())
	dst := mocks3.S3().Bucket(dstname)
	dst.PutBucket(s3.Private) // create it

	input := encodeKeys(mockbkt.Keys())
	var synced bytes.Buffer
	var failed bytes.Buffer

	err := sync.Sync(testlogger(t), input, &synced, &failed, src, dst, sync.Options{
		DecodePara: 3,
		SyncPara:   3,
		RetryBase:  time.Millisecond,
	})
	if err != nil {
		t.Fatalf("can't sync: %v", err)
	}

	if synced.Len() == 0 {
		t.Errorf("synced buffer should *not* be empty")
	}

	if failed.Len() != 0 {
		t.Errorf("failed buffer should be empty, but was: %v", failed.String())
	}

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

	syncKeys := decodeKeys(&synced)
	if len(want.Objects) != len(syncKeys) {
		t.Fatalf("want %d keys, got %d", len(want.Objects), len(syncKeys))
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

func TestSyncRecordsError(t *testing.T) {
	time.AfterFunc(time.Second*10, func() { panic("infinite loop?") })
	log.SetOutput(testwriter(t))

	mockbkt := s3mock.NewPerfBucket(t)
	mocks3 := s3mock.NewMock(t).Seed(mockbkt)
	defer mocks3.Close()

	dstname := "dst-bucket"
	src := mocks3.S3().Bucket(mockbkt.Name())
	dst := mocks3.S3().Bucket(dstname)
	dst.PutBucket(s3.Private) // create it

	input := encodeKeys(mockbkt.Keys())
	var synced bytes.Buffer
	var failed bytes.Buffer

	mocks3.SendErrors(
		1,   // send errors after 1 call (list bucket)
		1.0, // errors 100% of requests to S3
		[]s3.Error{
			{Message: s3.ErrInternalError},
			{Message: s3.ErrSlowDown},
			{Message: s3.ErrServiceUnavailable},
		})

	err := sync.Sync(testlogger(t), input, &synced, &failed, src, dst, sync.Options{
		DecodePara: 3,
		SyncPara:   3,
		RetryBase:  time.Millisecond,
	})
	if err != nil {
		t.Fatalf("can't sync: %v", err)
	}

	t.Logf("synced=%v", synced.String())
	t.Logf("failed=%v", failed.String())

	// all the keys should have failed
	if synced.Len() != 0 {
		t.Errorf("synced buffer should be empty, but was: %v", synced.String())
	}

	if failed.Len() == 0 {
		t.Errorf("failed buffer should *not* be empty")
	}

	want, ok := mocks3.ListBuckets()[mockbkt.Name()]
	if !ok {
		t.Fatalf("source bkt not found")
	}

	failKeys := decodeKeys(&failed)
	if len(want.Objects) != len(failKeys) {
		t.Fatalf("want %d failed keys, got %d", len(want.Objects), len(failKeys))
	}

	var wantKeys []s3.Key
	for _, wantobj := range want.Objects {
		wantKeys = append(wantKeys, wantobj.S3Key())
	}

	failKeys = sortKeys(failKeys)
	wantKeys = sortKeys(wantKeys)

	for i, wantKey := range wantKeys {
		gotKey := failKeys[i]
		// only compare key names, since the rest of wantKey
		// is random metadata from the fake s3 bucket (random bytes
		// gives random etags too)
		if wantKey.Key != gotKey.Key {
			t.Fatalf("key %d mistmatch, want %q, got %q", i, wantKey.Key, gotKey.Key)
		}
	}
}

func TestSyncSucceedWith50PercentErrors(t *testing.T) {
	time.AfterFunc(time.Second*10, func() { panic("infinite loop?") })
	log.SetOutput(testwriter(t))

	rand.Seed(42)

	mockbkt := s3mock.NewPerfBucket(t)
	mocks3 := s3mock.NewMock(t).Seed(mockbkt)
	defer mocks3.Close()

	dstname := "dst-bucket"
	src := mocks3.S3().Bucket(mockbkt.Name())
	dst := mocks3.S3().Bucket(dstname)
	dst.PutBucket(s3.Private) // create it

	input := encodeKeys(mockbkt.Keys())
	var synced bytes.Buffer
	var failed bytes.Buffer

	mocks3.SendErrors(
		1,    // send errors after 1 call (list bucket)
		0.50, // 50% of requests to S3 return errors
		[]s3.Error{
			{Message: s3.ErrInternalError},
			{Message: s3.ErrSlowDown},
			{Message: s3.ErrServiceUnavailable},
		})

	err := sync.Sync(testlogger(t), input, &synced, &failed, src, dst, sync.Options{
		DecodePara: 3,
		SyncPara:   3,
		RetryBase:  time.Microsecond * 500,
	})
	if err != nil {
		t.Fatalf("can't sync: %v", err)
	}

	t.Logf("synced=%v", synced.String())
	t.Logf("failed=%v", failed.String())

	if err != nil {
		t.Fatalf("can't sync: %v", err)
	}

	// all the keys should have eventually succeeded
	if synced.Len() == 0 {
		t.Errorf("synced buffer should *not* be empty")
	}

	if failed.Len() != 0 {
		t.Errorf("failed buffer should be empty, but was: %v", failed.String())
	}

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

	syncKeys := decodeKeys(&synced)
	if len(want.Objects) != len(syncKeys) {
		t.Fatalf("want %d keys, got %d", len(want.Objects), len(syncKeys))
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

// decode s3 keys from a json reader, fatals on error
func decodeKeys(in *bytes.Buffer) []s3.Key {
	var keys []s3.Key
	dec := json.NewDecoder(in)
	for {
		var key s3.Key
		err := dec.Decode(&key)
		if err == io.EOF {
			return keys
		} else if err != nil {
			log.Fatalf("encoding buf, %v", err)
		}
		keys = append(keys, key)
	}
}

// sort s3 keys lexicographically by key name
func sortKeys(orig []s3.Key) []s3.Key {
	keys := make([]s3.Key, len(orig))
	copy(keys, orig)
	ks := keyslice(keys)
	sort.Sort(&ks)
	return keys
}

type keyslice []s3.Key

func (k keyslice) Len() int           { return len(k) }
func (k keyslice) Swap(i, j int)      { k[i], k[j] = k[j], k[i] }
func (k keyslice) Less(i, j int) bool { return bytes.Compare([]byte(k[i].Key), []byte(k[j].Key)) == -1 }

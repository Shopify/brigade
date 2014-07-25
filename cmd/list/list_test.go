package list_test

import (
	"bytes"
	"encoding/json"
	"github.com/Shopify/brigade/cmd/list"
	"github.com/Shopify/brigade/s3mock"
	"github.com/Sirupsen/logrus"
	"github.com/aybabtme/goamz/s3"
	"io"
	"io/ioutil"
	"sort"
	"testing"
	"time"
)

func TestCanListBucket(t *testing.T) {
	withPerfBucket(t, func(t *testing.T, s3 *s3mock.MockS3, bkt s3mock.MockBucket, w io.Writer) error {
		return list.List(s3.S3(), "s3://"+bkt.Name(), w, true)
	})
}

func TestCanHandleS3Errors(t *testing.T) {

	mockBkt := s3mock.NewPerfBucket(t)
	mockS3 := s3mock.NewMock(t).Seed(mockBkt)

	// log the output to a buffer
	errout := bytes.NewBuffer(nil)
	logrus.SetOutput(errout)
	// will sleep for ~1s total, giving a chance to print 1 report
	list.MaxRetry = 5
	list.InitRetry = time.Millisecond * 8

	// closing will create a 'connection refused' error for every key
	mockS3.Close()

	// expectations:
	// no `list` call will succeed, so only one job will be created
	// - it will fail MaxRetry times
	wantRetrying := list.MaxRetry
	// - it will be abandoned once
	wantAbandon := 1

	err := list.List(mockS3.S3(), "s3://"+mockBkt.Name(), ioutil.Discard, true)
	if err != nil {
		t.Fatalf("%v", err)
	}

	t.Logf(errout.String())

	gotRetrying := bytes.Count(errout.Bytes(), []byte("retrying"))
	gotAbandon := bytes.Count(errout.Bytes(), []byte("abandon"))

	if gotRetrying != wantRetrying {
		t.Errorf("wanted %d retrying, got %d", wantRetrying, gotRetrying)
	}

	if gotAbandon != wantAbandon {
		t.Errorf("wanted %d abandon, got %d", wantAbandon, gotAbandon)
	}
}

// test context builders

func withPerfBucket(t *testing.T, f func(*testing.T, *s3mock.MockS3, s3mock.MockBucket, io.Writer) error) {

	mockBkt := s3mock.NewPerfBucket(t)
	mockS3 := s3mock.NewMock(t).Seed(mockBkt)
	defer mockS3.Close()
	output := bytes.NewBuffer(nil)

	if err := f(t, mockS3, mockBkt, output); err != nil {
		t.Fatalf("%v", err)
	}

	checkKeys(t, mockBkt.Keys(), output)
}

func checkKeys(t *testing.T, truth []s3.Key, gotKeybuf io.Reader) {
	wantKeys := sortKeys(truth)
	gotKeys := sortKeys(decodeKeys(gotKeybuf))

	if len(wantKeys) != len(gotKeys) {
		t.Fatalf("diff len, want %d got %d", len(wantKeys), len(gotKeys))
	}

	for i, wantKey := range wantKeys {
		gotKey := gotKeys[i]
		if gotKey.Key != wantKey.Key {
			t.Errorf("want key %q, got %q", wantKey.Key, gotKey.Key)
		}
	}
}

// decode s3 keys from a json reader, fatals on error
func decodeKeys(r io.Reader) []s3.Key {
	dec := json.NewDecoder(r)
	var key s3.Key
	var keys []s3.Key
	for {
		err := dec.Decode(&key)
		switch err {
		case io.EOF:
			return keys
		case nil:
			keys = append(keys, key)
		default:
			logrus.WithField("error", err).Fatal("decoding buf")
		}
	}
}

// sort s3 keys lexicographically
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

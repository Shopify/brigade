package diff_test

import (
	"bytes"
	"encoding/json"
	"github.com/Shopify/brigade/cmd/diff"
	"github.com/Sirupsen/logrus"
	"github.com/kr/pretty"
	"github.com/pushrax/goamz/s3"
	"io"
	"sort"
	"testing"
)

func init() {
	logrus.SetLevel(logrus.DebugLevel)
}

func TestCanDiffTwoKeySets(t *testing.T) {
	testDiff(t,
		[]s3.Key{{ETag: "1"}, {ETag: "3"}},              // old
		[]s3.Key{{ETag: "1"}, {ETag: "2"}, {ETag: "3"}}, // new
		[]s3.Key{{ETag: "2"}},                           // expected
	)
}

func TestCanDiffTwoEqualKeySets(t *testing.T) {
	testDiff(t,
		[]s3.Key{{ETag: "1"}}, // old
		[]s3.Key{{ETag: "1"}}, // new
		[]s3.Key{},            // expected
	)
}

func TestCanDiffTwoDifferentKeySets(t *testing.T) {
	testDiff(t,
		[]s3.Key{{ETag: "1"}}, // old
		[]s3.Key{{ETag: "2"}}, // new
		[]s3.Key{{ETag: "2"}}, // expected
	)
}

func TestCanDiffTwoEmptyKeySets(t *testing.T) {
	testDiff(t,
		[]s3.Key{}, // old
		[]s3.Key{}, // new
		[]s3.Key{}, // expected
	)
}

func TestCanDiffNewIsEmptyKeySets(t *testing.T) {
	testDiff(t,
		[]s3.Key{{ETag: "1"}}, // old
		[]s3.Key{},            // new
		[]s3.Key{},            // expected
	)
}

func TestCanDiffOldEmptyNewIsNotKeySets(t *testing.T) {
	testDiff(t,
		[]s3.Key{},            // old
		[]s3.Key{{ETag: "1"}}, // new
		[]s3.Key{{ETag: "1"}}, // expected
	)
}

func TestCanDiffWhenOldlistIsCorrupted(t *testing.T) {
	// setup
	oldKeys := []s3.Key{{ETag: "1"}, {ETag: "3"}}
	newKeys := []s3.Key{{ETag: "1"}, {ETag: "2"}, {ETag: "3"}}
	want := []s3.Key{{ETag: "2"}}

	oldbuf := encodeKeys(oldKeys)
	oldbuf.WriteString("garbage\ngarbage\ngarbage\ngarbage\ngarbage\ngarbage\ngarbage\ngarbage\ngarbage\ngarbage\ngarbage\n")
	oldList := lastErrReader(oldbuf, io.ErrUnexpectedEOF)
	newList := encodeKeys(newKeys)

	// execute
	output := bytes.NewBuffer(nil)
	err := diff.Diff(oldList, newList, output)

	// verify
	if err == nil {
		t.Fatalf("should have received an error, got nothing")
	} else {
		t.Logf("got error as expected: %v", err)
	}
	got := sortKeys(decodeKeys(output))
	checkKeys(t, want, got)
}

func TestCanDiffWhenNewlistIsCorrupted(t *testing.T) {
	// setup
	oldKeys := []s3.Key{{ETag: "1"}, {ETag: "3"}}
	newKeys := []s3.Key{{ETag: "1"}, {ETag: "2"}, {ETag: "3"}}
	want := []s3.Key{{ETag: "2"}}

	oldList := encodeKeys(oldKeys)
	newbuf := encodeKeys(newKeys)
	newbuf.WriteString("garbage\ngarbage\ngarbage\ngarbage\ngarbage\ngarbage\ngarbage\ngarbage\ngarbage\ngarbage\ngarbage\n")
	newList := lastErrReader(newbuf, io.ErrUnexpectedEOF)

	// execute
	output := bytes.NewBuffer(nil)
	err := diff.Diff(oldList, newList, output)

	// verify
	if err == nil {
		t.Fatalf("should have received an error, got nothing")
	} else {
		t.Logf("got error as expected: %v", err)
	}
	got := sortKeys(decodeKeys(output))
	checkKeys(t, want, got)
}

// context builders

func testDiff(t *testing.T, oldKeys, newKeys, want []s3.Key) {
	oldList := encodeKeys(oldKeys)
	newList := encodeKeys(newKeys)
	testDiffReaders(t, oldList, newList, want)
}

func testDiffReaders(t *testing.T, oldkeys, newkeys io.Reader, want []s3.Key) {
	output := bytes.NewBuffer(nil)
	err := diff.Diff(oldkeys, newkeys, output)
	if err != nil {
		t.Fatalf("failed to diff: %v", err)
	}
	got := sortKeys(decodeKeys(output))

	checkKeys(t, want, got)
}

func checkKeys(t *testing.T, want, got []s3.Key) {
	if len(want) != len(got) {
		t.Logf("want=%# v", pretty.Formatter(want))
		t.Logf("got=%# v", pretty.Formatter(got))
		t.Fatalf("want %d keys, got %d", len(want), len(got))
	}

	for i, gotk := range got {
		wantk := want[i]
		if wantk.ETag != gotk.ETag {
			t.Logf("want=%# v", pretty.Formatter(wantk))
			t.Logf("got=%# v", pretty.Formatter(gotk))
			t.Errorf("index %d differs", i)
		}
	}
}

// helpers

// io.Reader implementer
type reader func(p []byte) (int, error)

func (r reader) Read(p []byte) (int, error) { return r(p) }

func lastErrReader(r io.Reader, lastReadErr error) io.Reader {
	return reader(func(p []byte) (int, error) {
		n, err := r.Read(p)
		if err == io.EOF {
			return n, lastReadErr
		}
		return n, err
	})
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

// encode s3 keys from a json writer, fatals on error
func encodeKeys(keys []s3.Key) *bytes.Buffer {
	out := bytes.NewBuffer(nil)
	enc := json.NewEncoder(out)
	for _, key := range keys {
		err := enc.Encode(&key)
		if err != nil {
			logrus.WithField("error", err).Fatal("encoding buf")
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

func (k keyslice) Len() int      { return len(k) }
func (k keyslice) Swap(i, j int) { k[i], k[j] = k[j], k[i] }
func (k keyslice) Less(i, j int) bool {
	return bytes.Compare([]byte(k[i].ETag), []byte(k[j].ETag)) == -1
}

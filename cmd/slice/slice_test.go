package slice_test

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"github.com/Shopify/brigade/cmd/slice"
	"github.com/Sirupsen/logrus"
	"github.com/kr/pretty"
	"github.com/pushrax/goamz/s3"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"testing"
)

func TestCanSlice(t *testing.T) {
	keys := []s3.Key{
		{ETag: "1"}, {ETag: "2"}, {ETag: "3"}, {ETag: "4"}, {ETag: "5"}, {ETag: "6"},
	}
	want := [][]s3.Key{
		{{ETag: "1"}},
		{{ETag: "2"}},
		{{ETag: "3"}},
		{{ETag: "4"}},
		{{ETag: "5"}},
		{{ETag: "6"}},
	}
	checkSlice(t, keys, want)
}

func TestCanSliceEmptyK(t *testing.T) {
	keys := []s3.Key{
	// nothing
	}
	want := [][]s3.Key{
		{},
		{},
		{},
		{},
		{},
		{},
	}
	checkSlice(t, keys, want)
}

func TestCanSliceWithOverflow1(t *testing.T) {
	keys := []s3.Key{
		{ETag: "1"}, {ETag: "2"}, {ETag: "3"}, {ETag: "4"}, {ETag: "5"}, {ETag: "6"}, {ETag: "7"},
	}
	want := [][]s3.Key{
		{{ETag: "1"}, {ETag: "7"}},
		{{ETag: "2"}},
		{{ETag: "3"}},
		{{ETag: "4"}},
		{{ETag: "5"}},
		{{ETag: "6"}},
	}
	checkSlice(t, keys, want)
}

func TestCanSliceWithOverflow2(t *testing.T) {
	keys := []s3.Key{
		{ETag: "1"}, {ETag: "2"}, {ETag: "3"}, {ETag: "4"}, {ETag: "5"}, {ETag: "6"}, {ETag: "7"}, {ETag: "8"},
	}
	want := [][]s3.Key{
		{{ETag: "1"}, {ETag: "7"}},
		{{ETag: "2"}, {ETag: "8"}},
		{{ETag: "3"}},
		{{ETag: "4"}},
		{{ETag: "5"}},
		{{ETag: "6"}},
	}
	checkSlice(t, keys, want)
}

func TestCanSliceWithAllOverflow(t *testing.T) {
	keys := []s3.Key{
		{ETag: "1"}, {ETag: "2"}, {ETag: "3"}, {ETag: "4"}, {ETag: "5"}, {ETag: "6"}, {ETag: "7"}, {ETag: "8"},
	}
	want := [][]s3.Key{
		{{ETag: "1"}, {ETag: "2"}, {ETag: "3"}, {ETag: "4"}, {ETag: "5"}, {ETag: "6"}, {ETag: "7"}, {ETag: "8"}},
	}
	checkSlice(t, keys, want)
}

func TestCanSliceWithHalfOverflow(t *testing.T) {
	keys := []s3.Key{
		{ETag: "1"}, {ETag: "2"}, {ETag: "3"}, {ETag: "4"}, {ETag: "5"}, {ETag: "6"}, {ETag: "7"}, {ETag: "8"},
	}
	want := [][]s3.Key{
		{{ETag: "1"}, {ETag: "3"}, {ETag: "5"}, {ETag: "7"}},
		{{ETag: "2"}, {ETag: "4"}, {ETag: "6"}, {ETag: "8"}},
	}
	checkSlice(t, keys, want)
}

// Test builders/checkers

func checkSlice(t *testing.T, keys []s3.Key, want [][]s3.Key) {
	withSourceFile(t, keys, func(filename string) {
		var err error
		filenames, err := slice.Slice(filename, len(want))
		if err != nil {
			t.Errorf("failed to slice: %v", err)
			return
		}
		checkSubfiles(t, want, filenames)
	})
}

func withSourceFile(t *testing.T, sourceKeys []s3.Key, f func(string)) {

	srcFile, err := ioutil.TempFile(os.TempDir(), "slice_test")
	if err != nil {
		t.Fatalf("couldn't create test source file: %v", err)
	}
	defer os.Remove(srcFile.Name())

	gzw := gzip.NewWriter(srcFile)

	buf := encodeKeys(sourceKeys)
	_, err = buf.WriteTo(gzw)
	if err != nil {
		t.Errorf("couldn't write output to test source file: %v", err)
		return
	}
	err = gzw.Close()
	if err != nil {
		t.Errorf("couldn't close gzip file output: %v", err)
		return
	}
	err = srcFile.Close()
	if err != nil {
		t.Errorf("couldn't close test source file: %v", err)
		return
	}

	f(srcFile.Name())

}

func checkSubfiles(t *testing.T, subkeys [][]s3.Key, filenames []string) {

	if len(subkeys) != len(filenames) {
		t.Errorf("wanted %d subfiles, got %d", len(subkeys), len(filenames))
		return
	}

	for i, subfilename := range filenames {
		file, err := os.Open(subfilename)
		if err != nil {
			t.Errorf("couldn't open %q, %v", subfilename, err)
			continue
		}
		defer func(subfilename string) {
			err := os.Remove(subfilename)
			if err != nil {
				t.Errorf("failed to remove %q: %v", subfilename, err)
			}
		}(subfilename)

		gzr, err := gzip.NewReader(file)
		if err != nil {
			t.Errorf("couldn't get gzip from %q, %v", subfilename, err)
			continue
		}

		want := sortKeys(subkeys[i])
		got := sortKeys(decodeKeys(gzr))
		checkKeys(t, want, got)
		_ = gzr.Close()
		_ = file.Close()
	}
}

func checkKeys(t *testing.T, want, got []s3.Key) {
	if len(want) != len(got) {
		t.Logf("want=%# v", pretty.Formatter(want))
		t.Logf("got=%# v", pretty.Formatter(got))
		t.Errorf("want %d keys, got %d", len(want), len(got))
		return
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
			logrus.WithField("error", err).Error("decoding buffer")
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
			logrus.WithField("error", err).Error("encoding buffer")
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

package backup

import (
	"bytes"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/aybabtme/goamz/aws"
	"github.com/aybabtme/goamz/s3"

	"testing"
	"time"
)

func TestPrepareChunk(t *testing.T) {
	cases := []struct {
		name           string
		size, partSize int64
		chunks         []*byteChunk
	}{
		{
			name: "many chunks, aligned",
			size: 64, partSize: 16,
			chunks: []*byteChunk{
				{seq: 0, start: 0, end: 16},
				{seq: 1, start: 16, end: 32},
				{seq: 2, start: 32, end: 48},
				{seq: 3, start: 48, end: 64},
			},
		},
		{
			name: "many chunks, disaligned",
			size: 65, partSize: 16,
			chunks: []*byteChunk{
				{seq: 0, start: 0, end: 16},
				{seq: 1, start: 16, end: 32},
				{seq: 2, start: 32, end: 48},
				{seq: 3, start: 48, end: 64},
				{seq: 4, start: 64, end: 65},
			},
		},
		{
			name: "one chunk, aligned",
			size: 64, partSize: 64,
			chunks: []*byteChunk{
				{seq: 0, start: 0, end: 64},
			},
		},
		{
			name: "one chunk, misaligned",
			size: 63, partSize: 64,
			chunks: []*byteChunk{
				{seq: 0, start: 0, end: 63},
			},
		},
	}
	for _, tcase := range cases {
		t.Log(tcase.name)
		gotChunks := prepareChunks(tcase.size, tcase.partSize)

		if len(tcase.chunks) != len(gotChunks) {
			t.Fatalf("want %d chunks, got %d", len(tcase.chunks), len(gotChunks))
		}

		for i, want := range tcase.chunks {
			got := gotChunks[i]
			if want.seq != got.seq {
				t.Errorf("want seq %d, got %d", want.seq, got.seq)
			}
			if want.start != got.start {
				t.Errorf("want start %d, got %d", want.start, got.start)
			}
			if want.end != got.end {
				t.Errorf("want end %d, got %d", want.end, got.end)
			}
		}
	}
}

func TestSmokeTestFetchRealFile(t *testing.T) { withAWSBucket(t, smokeTestPutThenFetch) }
func TestChunkFetch_100B(t *testing.T)        { withAWSBucket(t, putThenFetch(100)) }
func TestChunkFetch_1KB(t *testing.T)         { withAWSBucket(t, putThenFetch(1<<10)) }
func TestChunkFetch_1MB(t *testing.T)         { withAWSBucket(t, putThenFetch(1<<20)) }
func TestChunkFetch_10MB(t *testing.T)        { withAWSBucket(t, putThenFetch(1<<20*10)) }
func TestChunkFetch_100MB(t *testing.T)       { withAWSBucket(t, putThenFetch(1<<20*100)) }
func TestChunkFetch_500MB(t *testing.T)       { withAWSBucket(t, putThenFetch(1<<20*500)) }
func TestChunkFetch_1700MB(t *testing.T)      { withAWSBucket(t, putThenFetch(1<<20*1700)) }

func withAWSBucket(t *testing.T, action func(t *testing.T, bkt *s3.Bucket) error) {
	if testing.Short() {
		t.Skipf("testing integration tests with AWS")
	}

	auth, err := aws.EnvAuth()
	if err != nil {
		t.Skipf("skipping due to AWS auth: %v", err)
	}

	logrus.SetLevel(logrus.DebugLevel)

	logrus.Debug("test: preparing...")

	s := s3.New(auth, aws.USEast)
	s.ReadTimeout = time.Second * 10
	s.ConnectTimeout = time.Second * 10
	bkt := s.Bucket(fmt.Sprintf("shopify-brigade-integration-test-delete-me-%d", time.Now().Unix()))
	if err := bkt.PutBucket(s3.PublicRead); err != nil {
		t.Fatalf("bucket cannot be created: %v", err)
	}
	defer func() {
		logrus.Debug("test: cleaning up...")
		l, err := bkt.List("", "/", "/", 100000)
		if err != nil {
			t.Errorf("bucket not listable: %v", err)
		}

		for _, key := range l.Contents {
			if err := bkt.Del(key.Key); err != nil {
				t.Errorf("deleting %q: %v", key.Key, err)
			}
		}
		if err := bkt.DelBucket(); err != nil {
			t.Fatalf("couldn't delete bucket %q: %v", bkt.Name, err)
		}
	}()

	l, err := bkt.List("", "/", "/", 10)
	if err != nil {
		t.Fatalf("bucket not listable: %v", err)
	}
	if len(l.Contents) != 0 {
		t.Fatalf("bucket not empty")
	}

	logrus.Debug("test: starting...")
	err = action(t, bkt)
	logrus.Debug("test: completed")
	if err != nil {
		t.Fatalf("general test error: %v", err)
	}
}

func smokeTestPutThenFetch(t *testing.T, bkt *s3.Bucket) error {
	want := bytes.Repeat([]byte("helloworld!"), 1<<10) // 1kb
	path := "hello"
	err := bkt.PutReader(path,
		bytes.NewReader(want),
		int64(len(want)),
		"",
		s3.PublicRead,
		s3.Options{},
	)
	if err != nil {
		return err
	}

	got, err := bkt.Get(path)
	if err != nil {
		return err
	}
	if !bytes.Equal(want, got) {
		return fmt.Errorf("want %d bytes, got %d", len(want), len(got))
	}

	return nil
}

func putThenFetch(size int) func(t *testing.T, bkt *s3.Bucket) error {
	return func(t *testing.T, bkt *s3.Bucket) error {
		want := bytes.Repeat([]byte("0"), size)
		path := "hello"

		err := multipartPut(
			bkt,
			path,
			bytes.NewReader(want),
			int64(len(want)),
		)
		if err != nil {
			return err
		}

		key := s3.Key{Key: path, Size: int64(len(want))}
		getter := NewChunkGetter(bkt, key, size/10)
		buf := bytes.NewBuffer(nil)
		_, err = getter.WriteTo(buf)
		if err != nil {
			return err
		}
		got := buf.Bytes()
		logrus.Debug("test: checking answer (slow for large data)")

		if bytes.Equal(want, got) {
			return nil
		}

		logrus.Debug("not equal!")

		if len(want) != len(got) {
			return fmt.Errorf("different size, want %d got %d", len(want), len(got))
		}

		diff := 0
		for i := range want {
			if want[i] != got[i] {
				logrus.WithField("i", i).Debug("byte differ")
				t.Errorf("bytes %d differ, want %v got %v", i, want[i], got[i])
				diff++
			}

			if diff > 10 {
				return nil
			}
		}
		return nil
	}
}

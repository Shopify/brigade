package backup

import (
	"bytes"
	"errors"
	"github.com/Shopify/brigade/s3mock"
	"github.com/Sirupsen/logrus"
	"github.com/aybabtme/goamz/s3"
	"io/ioutil"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"
	"time"
)

func timeGenerator(vals []reflect.Value, rand *rand.Rand) {
	months := []time.Month{
		time.January, time.February, time.March,
		time.April, time.May, time.June,
		time.July, time.August, time.September,
		time.October, time.November, time.December,
	}

	for i := range vals {
		vals[i] = reflect.ValueOf(time.Date(
			rand.Intn(9999),
			months[rand.Intn(len(months))],
			rand.Intn(31),
			rand.Intn(24),
			rand.Intn(60),
			rand.Intn(60),
			0, // ignore nanosecs
			time.UTC,
		))
	}
}

func TestToAndFromName(t *testing.T) {
	qconf := &quick.Config{
		MaxCount: 10000,
		Rand:     rand.New(rand.NewSource(int64(42))),
		Values:   timeGenerator,
	}

	for _, conv := range []struct {
		to   func(time.Time) string
		from func(string) (time.Time, bool, error)
	}{
		{toSourceName, fromSourceName},
		{toDiffName, fromDiffName},
		{toOkName, fromOkName},
		{toFailName, fromFailName},
	} {
		err := quick.Check(func(want time.Time) bool {
			name := conv.to(want)
			got, found, err := conv.from(name)
			if err != nil {
				t.Errorf("error: with want=%v, name=%q: %v", want, name, err)
				return false
			}
			if !found {
				t.Errorf("time string not found: with want=%v, name=%q", want, name)
				return false
			}
			if !got.Equal(want) {
				t.Errorf("mismatch: with want=%v, name=%q, got=%v", want, name, got)
				return false
			}
			return true
		}, qconf)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestFindLastList(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)

	s3m := s3mock.NewMock(t)
	defer s3m.Close()
	perf := s3mock.NewPerfBucket(t)
	s3m.Seed(perf)

	t.Logf("should have nothing in the bucket to begin with")
	bkt := s3m.S3().Bucket(perf.Name())
	if found, err := findLastList(bkt, "", nil); err != nil || found {
		t.Fatalf("should not have found anything: found=%v err=%v", found, err)
	}

	t.Logf("add a source file")
	want := []byte("hello world")
	start := time.Now()
	name := toSourceName(start)
	if err := bkt.Put(name, want, "", s3.BucketOwnerFull, s3.Options{}); err != nil {
		t.Fatal(err)
	}

	t.Logf("check it's there")
	got := helpFindLast(t, bkt)
	if !bytes.Equal(want, got) {
		t.Fatalf("want %v got %v", want, got)
	}

	t.Logf("add an older one")
	dontWant := []byte("bye world")
	oldname := toSourceName(start.AddDate(-1, 0, 0))
	if err := bkt.Put(oldname, dontWant, "", s3.BucketOwnerFull, s3.Options{}); err != nil {
		t.Fatal(err)
	}

	t.Logf("check we still get the most recent one there")
	got = helpFindLast(t, bkt)
	if !bytes.Equal(want, got) {
		t.Fatalf("want %v got %v", want, got)
	}
	if bytes.Equal(dontWant, got) {
		t.Fatalf("dont want %v got %v", want, got)
	}

	t.Logf("add a more recent one")
	newWant := []byte("brave new world")
	recentName := toSourceName(start.AddDate(1, 0, 0))
	if err := bkt.Put(recentName, newWant, "", s3.BucketOwnerFull, s3.Options{}); err != nil {
		t.Fatal(err)
	}

	t.Logf("check we now get the newly put one")
	got = helpFindLast(t, bkt)
	if bytes.Equal(want, got) {
		t.Fatalf("dont want %v got %v", want, got)
	}
	if bytes.Equal(dontWant, got) {
		t.Fatalf("dont want %v got %v", want, got)
	}
	if !bytes.Equal(newWant, got) {
		t.Fatalf("want %v got %v", want, got)
	}

}

func helpFindLast(t *testing.T, bkt *s3.Bucket) []byte {
	rd := newByteSeeker()
	found, err := findLastList(bkt, "", rd)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatalf("should have found something")
	}
	var got []byte
	got, err = ioutil.ReadAll(rd)
	rd.Close()
	if err != nil {
		t.Fatal(err)
	}
	return got
}

type byteSeeker struct {
	buf *bytes.Buffer
}

func newByteSeeker() *byteSeeker {
	return &byteSeeker{
		buf: bytes.NewBuffer(nil),
	}
}

func (b *byteSeeker) Write(p []byte) (int, error) { return b.buf.Write(p) }
func (b *byteSeeker) Read(p []byte) (int, error)  { return b.buf.Read(p) }
func (b *byteSeeker) Close() error                { return nil }

// only ever resets the buffer to offset 0
func (b *byteSeeker) Seek(offset int64, whence int) (int64, error) {
	if offset != 0 || whence != 0 {
		return -1, errors.New("can only invoke byteSeeker.Seek with 0 offset, 0 whence")
	}
	b.buf = bytes.NewBuffer(b.buf.Bytes())
	return 0, nil
}

package diff

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/aybabtme/goamz/s3"
	"github.com/aybabtme/uniplot/spark"
	"github.com/bradfitz/iter"
	"github.com/dustin/go-humanize"
	"io"
	"log"
	"runtime"
	"sync"
	"time"
)

var (
	bufferFactor = 10
)

type diffTask struct {
	elog *log.Logger
}

// Diff reads s3 keys in JSON form from old and new list, compute which
// keys have changed from the old to the new one, writing those keys
// to the output writer (in JSON as well).
func Diff(el *log.Logger, oldList, newList io.Reader, output io.Writer) error {

	differ := diffTask{elog: el}

	start := time.Now()

	log.Printf("computing difference in keys")
	diff, differr := differ.computeDifference(oldList, newList)
	if differr != nil && len(diff) == 0 {
		return fmt.Errorf("computing source difference, produced no diff, %v", differr)
	}

	log.Printf("done computing difference in %v: %s keys differ", time.Since(start), humanize.Comma(int64(len(diff))))

	writeerr := differ.writeDiff(output, diff)

	log.Printf("done writing difference in %v", time.Since(start))

	switch {
	case differr != nil && writeerr != nil:
		return fmt.Errorf("diffing sources, %v; writing diff, %v", differr, writeerr)
	case differr != nil:
		return fmt.Errorf("reading diff sources (could still write partial diff), %v", differr)
	case writeerr != nil:
		return fmt.Errorf("writing diff, %v", writeerr)
	default:
		return nil
	}
}

func (dt *diffTask) computeDifference(oldList, newList io.Reader) ([]s3.Key, error) {

	log.Printf("reading old key list...")
	knownKeys, olderr := dt.readOldList(oldList)
	switch olderr {
	case io.ErrUnexpectedEOF:
		dt.elog.Printf("reading old source: %v", olderr)
		// carry on
	case nil:
		// carry on
	default:
		// not nil, not UnexpectedEOF; bail
		return nil, fmt.Errorf("reading old source, %v", olderr)
	}

	log.Printf("old list contains %s keys", humanize.Comma(int64(knownKeys.Len())))

	log.Printf("reading new key list...")
	diff, listlen, newerr := dt.filterNewKeys(newList, knownKeys)
	switch newerr {
	case io.ErrUnexpectedEOF:
		dt.elog.Printf("reading new source: %v", newerr)
		// carry on
	case nil:
		// carry on
	default:
		// not nil, not UnexpectedEOF; bail
		return nil, fmt.Errorf("reading first source, %v", newerr)
	}

	log.Printf("new list contains %s keys", humanize.Comma(int64(listlen)))

	switch {
	case olderr != nil && newerr != nil:
		return diff, fmt.Errorf("reading both source, %v", olderr)
	case olderr != nil:
		return diff, fmt.Errorf("reading old source, %v", olderr)
	case newerr != nil:
		return diff, fmt.Errorf("reading new source, %v", newerr)
	default:
		return diff, nil
	}
}

func (dt *diffTask) readOldList(src io.Reader) (keySet, error) {
	keyset := newKeyMap()
	decoders := make(chan []byte, runtime.NumCPU()*bufferFactor)
	keys := make(chan s3.Key, runtime.NumCPU()*bufferFactor)

	doneSrcA := make(chan struct{})
	go func() {
		sprk := spark.Spark(time.Millisecond * 60)
		defer close(doneSrcA)
		sprk.Start()
		sprk.Units = "keys"
		for key := range keys {
			// stores the etag, which will differ if files have changed
			keyset.Add(key.ETag)
			sprk.Add(1.0)
		}
		sprk.Stop()
	}()

	wg := sync.WaitGroup{}
	for _ = range iter.N(runtime.NumCPU()) {
		wg.Add(1)
		go dt.decode(&wg, decoders, keys)
	}

	if err := dt.readLines(src, decoders); err != nil {
		wg.Wait()
		close(keys)
		<-doneSrcA
		return keyset, err
	}
	wg.Wait()
	close(keys)
	<-doneSrcA

	return keyset, nil
}

func (dt *diffTask) filterNewKeys(src io.Reader, keyset keySet) ([]s3.Key, int, error) {

	decoders := make(chan []byte, runtime.NumCPU()*bufferFactor)
	keys := make(chan s3.Key, runtime.NumCPU()*bufferFactor)

	var diffKeys []s3.Key
	var newKeys int

	doneDiffing := make(chan struct{})
	go func() {
		sprk := spark.Spark(time.Millisecond * 60)
		defer close(doneDiffing)
		sprk.Start()
		sprk.Units = "keys"
		for key := range keys {
			newKeys++
			sprk.Add(1.0)
			// only add ETags that aren't known from the old list
			if !keyset.Contains(key.ETag) {
				diffKeys = append(diffKeys, key)
			}
		}
		sprk.Stop()
	}()

	wg := sync.WaitGroup{}
	for _ = range iter.N(runtime.NumCPU()) {
		wg.Add(1)
		go dt.decode(&wg, decoders, keys)
	}

	if err := dt.readLines(src, decoders); err != nil {
		wg.Wait()
		close(keys)
		<-doneDiffing
		return diffKeys, newKeys, err
	}

	wg.Wait()
	close(keys)
	<-doneDiffing

	return diffKeys, newKeys, nil
}

func (dt *diffTask) writeDiff(w io.Writer, keys []s3.Key) error {
	enc := json.NewEncoder(w)
	for i, k := range keys {
		err := enc.Encode(k)
		if err != nil {
			return fmt.Errorf("failed to encode key %d/%d, %v", i+1, len(keys), err)
		}
	}
	return nil
}

func (dt *diffTask) readLines(r io.Reader, decoders chan<- []byte) error {
	defer close(decoders)
	rd := bufio.NewReader(r)
	for {
		line, err := rd.ReadBytes('\n')
		switch err {
		case io.EOF:
			return nil
		case nil:
		default:
			return err
		}

		decoders <- line
	}
}

// decodes s3.Keys from a channel of bytes, each byte containing a full key
func (dt *diffTask) decode(wg *sync.WaitGroup, lines <-chan []byte, keys chan<- s3.Key) {
	defer wg.Done()
	var key s3.Key
	for line := range lines {
		err := json.Unmarshal(line, &key)
		if err != nil {
			dt.elog.Printf("unmarshaling line: %v", err)
		} else {
			keys <- key
		}
	}
}

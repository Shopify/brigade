package diff

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/aybabtme/uniplot/spark"
	"github.com/bradfitz/iter"
	"github.com/crowdmob/goamz/s3"
	"github.com/dustin/go-humanize"
	"io"
	"log"
	"runtime"
	"sync"
	"time"
)

var (
	bufferFactor = 10
	elog         *log.Logger
)

// Diff reads s3 keys in JSON form from old and new list, compute which
// keys have changed from the old to the new one, writing those keys
// to the output writer (in JSON as well).
func Diff(el *log.Logger, oldList, newList io.Reader, output io.Writer) error {
	elog = el

	start := time.Now()
	log.Printf("computing difference in keys")
	diff, err := computeDifference(oldList, newList)
	if err != nil && len(diff) == 0 {
		return fmt.Errorf("computing source difference, produced no diff, %v", err)
	} else if err != nil {
		elog.Printf("an error occured computing difference: %v", err)
	}
	log.Printf("done computing difference in %v: %s keys differ", time.Since(start), humanize.Comma(int64(len(diff))))

	if err := writeDiff(output, diff); err != nil {
		return fmt.Errorf("writing difference to output, %v", err)
	}
	log.Printf("done writing difference in %v", time.Since(start))
	return nil
}

func computeDifference(oldList, newList io.Reader) ([]s3.Key, error) {

	keyset, err := readOldList(oldList)
	if err != nil {
		return nil, fmt.Errorf("reading first source, %v", err)
	}
	log.Printf("old list contains %s keys", humanize.Comma(int64(keyset.Len())))

	diff, listlen, err := readNewList(newList, keyset)
	log.Printf("new list contains %s keys", humanize.Comma(int64(listlen)))
	if err != nil {
		return diff, fmt.Errorf("reading second source, %v", err)
	}
	return diff, nil
}

func readOldList(src io.Reader) (keySet, error) {
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
		go decode(&wg, decoders, keys)
	}

	if err := readLines(src, decoders); err != nil {
		return nil, err
	}
	wg.Wait()
	close(keys)
	<-doneSrcA

	return keyset, nil
}

func readNewList(src io.Reader, keyset keySet) ([]s3.Key, int, error) {

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
		go decode(&wg, decoders, keys)
	}

	if err := readLines(src, decoders); err != nil {
		return diffKeys, newKeys, err
	}
	wg.Wait()
	close(keys)
	<-doneDiffing

	return diffKeys, newKeys, nil
}

func writeDiff(w io.Writer, keys []s3.Key) error {
	enc := json.NewEncoder(w)
	for i, k := range keys {
		err := enc.Encode(k)
		if err != nil {
			return fmt.Errorf("failed to encode key %d/%d, %v", i+1, len(keys), err)
		}
	}
	return nil
}

func readLines(r io.Reader, decoders chan<- []byte) error {
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
func decode(wg *sync.WaitGroup, lines <-chan []byte, keys chan<- s3.Key) {
	defer wg.Done()
	var key s3.Key
	for line := range lines {
		err := json.Unmarshal(line, &key)
		if err != nil {
			elog.Printf("unmarshaling line: %v", err)
		} else {
			keys <- key
		}
	}
}

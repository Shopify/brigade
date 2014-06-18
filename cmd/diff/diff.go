package diff

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/bradfitz/iter"
	"github.com/crowdmob/goamz/s3"
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
	if err != nil {
		return fmt.Errorf("computing source difference, %v", err)
	}
	log.Printf("done computing difference in %v: %d keys differ", time.Since(start), len(diff))

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
	log.Printf("old list contains %d keys", keyset.Len())

	diff, listlen, err := readNewList(newList, keyset)
	if err != nil {
		return nil, fmt.Errorf("reading second source, %v", err)
	}
	log.Printf("new list contains %d keys", listlen)
	return diff, nil
}

func readOldList(src io.Reader) (keySet, error) {
	keyset := newKeyMap()
	decoders := make(chan []byte, runtime.NumCPU()*bufferFactor)
	keys := make(chan s3.Key, runtime.NumCPU()*bufferFactor)

	doneSrcA := make(chan struct{})
	go func() {
		defer close(doneSrcA)
		for key := range keys {
			// stores the etag, which will differ if files have changed
			keyset.Add(key.ETag)
		}
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
		defer close(doneDiffing)
		for key := range keys {
			newKeys++
			// only add ETags that aren't known from the old list
			if !keyset.Contains(key.ETag) {
				diffKeys = append(diffKeys, key)
			}
		}
	}()

	wg := sync.WaitGroup{}
	for _ = range iter.N(runtime.NumCPU()) {
		wg.Add(1)
		go decode(&wg, decoders, keys)
	}

	if err := readLines(src, decoders); err != nil {
		return nil, newKeys, err
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

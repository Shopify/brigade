package diff

import (
	"bufio"
	"encoding/json"
	"expvar"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/pushrax/goamz/s3"
	"io"
	"runtime"
	"sync"
	"time"
)

var (
	bufferFactor = 10
)

type diffTask struct{}

var metrics = struct {
	oldKeys     *expvar.Int
	diffKeys    *expvar.Int
	newKeys     *expvar.Int
	encodedKeys *expvar.Int
}{
	oldKeys:     expvar.NewInt("brigade.diff.oldKeys"),
	diffKeys:    expvar.NewInt("brigade.diff.diffKeys"),
	newKeys:     expvar.NewInt("brigade.diff.newKeys"),
	encodedKeys: expvar.NewInt("brigade.diff.encodedKeys"),
}

// Diff reads s3 keys in JSON form from old and new list, compute which
// keys have changed from the old to the new one, writing those keys
// to the output writer (in JSON as well).
func Diff(oldList, newList io.Reader, output io.Writer) error {

	var differ diffTask

	start := time.Now()

	logrus.Info("computing difference in keys")
	diff, differr := differ.computeDifference(oldList, newList)
	if differr != nil && len(diff) == 0 {
		return fmt.Errorf("computing source difference, produced no diff, %v", differr)
	}

	logrus.WithFields(logrus.Fields{
		"duration":       time.Since(start),
		"key_difference": len(diff),
	}).Info("done computing difference in keys")

	writeerr := differ.writeDiff(output, diff)

	logrus.WithField("duration", time.Since(start)).Info("done writing difference to file")

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

	logrus.Info("reading old key list...")
	knownKeys, olderr := dt.readOldList(oldList)
	switch olderr {
	case io.ErrUnexpectedEOF:
		logrus.WithField("error", olderr).Error("reading old source")
		// carry on
	case nil:
		// carry on
	default:
		// not nil, not UnexpectedEOF; bail
		return nil, fmt.Errorf("reading old source, %v", olderr)
	}

	logrus.WithField("key_count", knownKeys.Len()).Info("done reading keys from old source")

	logrus.Info("reading new key list...")
	diff, listlen, newerr := dt.filterNewKeys(newList, knownKeys)
	switch newerr {
	case io.ErrUnexpectedEOF:
		logrus.WithField("error", newerr).Error("reading new source")
		// carry on
	case nil:
		// carry on
	default:
		// not nil, not UnexpectedEOF; bail
		return nil, fmt.Errorf("reading first source, %v", newerr)
	}

	logrus.WithField("key_count", listlen).Info("done reading keys from new source")

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
		defer close(doneSrcA)
		for key := range keys {
			metrics.oldKeys.Add(1)
			// stores the etag, which will differ if files have changed
			keyset.Add(key.ETag)
		}
	}()

	wg := sync.WaitGroup{}
	for i := 0; i < runtime.NumCPU(); i++ {
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
		defer close(doneDiffing)
		for key := range keys {
			metrics.newKeys.Add(1)
			newKeys++
			// only add ETags that aren't known from the old list
			if !keyset.Contains(key.ETag) {
				metrics.diffKeys.Add(1)
				diffKeys = append(diffKeys, key)
			}
		}
	}()

	wg := sync.WaitGroup{}
	for i := 0; i < runtime.NumCPU(); i++ {
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
		metrics.encodedKeys.Add(1)
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
			logrus.WithField("error", err).Error("failed to unmarshal s3.Key from line")
		} else {
			keys <- key
		}
	}
}

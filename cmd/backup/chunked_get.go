package backup

import (
	"bufio"
	"bytes"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/aybabtme/goamz/s3"
	"io"
	"math"
	"net/http"
)

type ChunkGetter struct {
	key      s3.Key
	bkt      *s3.Bucket
	maxRetry int
	partSize int64
}

type byteChunk struct {
	seq, start, end int64
	data            []byte
	content         chan *bytes.Reader
}

func (b byteChunk) String() string { return b.GoString() }
func (b byteChunk) GoString() string {
	return fmt.Sprintf(`{seq=%d start=%d end=%d data=%.1f%%}`,
		b.seq, b.start, b.end, float64(len(b.data))/float64(b.end-b.start)*100)
}

func newByteChunk(seq, start, end int64) *byteChunk {
	return &byteChunk{
		seq: seq, start: start, end: end,
		content: make(chan *bytes.Reader, 1),
	}
}

func prepareChunks(size, partSize int64) []*byteChunk {
	count := int(math.Ceil(float64(size) / float64(partSize)))
	chunks := make([]*byteChunk, count)
	var start int64
	var end int64
	for i := range chunks {
		start = (int64(i) * partSize)
		end = imin(((int64(i) + 1) * partSize), size)
		chunks[i] = newByteChunk(int64(i), start, end)
	}
	return chunks
}

func NewChunkGetter(bkt *s3.Bucket, key s3.Key, partSize int) *ChunkGetter {
	return &ChunkGetter{key: key,
		bkt:      bkt,
		maxRetry: 10,
		partSize: int64(partSize),
	}
}

func (c *ChunkGetter) WriteTo(dst io.Writer) (int64, error) {

	byteChunks := prepareChunks(c.key.Size, c.partSize)

	log.WithField("count", len(byteChunks)).Debug("prepared byte chunks")

	killall := make(chan struct{})
	errc := make(chan error, len(byteChunks))
	defer close(killall)

	for _, chunk := range byteChunks {
		go func(chnk *byteChunk) {
			if err := c.fetchChunkWithRetry(chnk, killall); err != nil {
				errc <- err
			}
		}(chunk)
	}
	var written int64
	for i, chunk := range byteChunks {
		log.WithField("chunk", chunk.seq).Debug("waiting for chunk")
		if int64(i) != chunk.seq {
			log.WithFields(log.Fields{
				"index": i,
				"seq":   chunk.seq,
			}).Panic("unexpected chunk sequence number")
		}
		content, open := <-chunk.content
		if !open {
			return 0, <-errc
		}
		log.WithField("chunk", chunk).Debug("received")
		m := content.Len()
		n, err := io.Copy(dst, content)
		written += n
		if err != nil {
			return written, err
		}
		if n != int64(m) {
			return written, fmt.Errorf("short write, want %d wrote %d", m, n)
		}
		log.WithField("chunk", chunk.seq).Debug("chunk written")
	}
	close(errc)
	log.Debug("done getting chunks")
	return written, <-errc
}

func (c *ChunkGetter) fetchChunkWithRetry(chunk *byteChunk, killall <-chan struct{}) error {
	defer close(chunk.content)
	try := 0
	var err error
	for try < c.maxRetry {
		select {
		case <-killall:
			return nil
		default:
		}
		err = c.doFetchChunk(chunk)
		if err == nil {
			return nil
		}
		log.WithFields(log.Fields{
			"error": err,
			"chunk": chunk,
			"try":   try,
		}).Debug("error fetching")
		try++
	}
	return fmt.Errorf("chunk %d [%d, %d] failed too many times: %v", chunk.seq, chunk.start, chunk.end, err)
}

func (c *ChunkGetter) doFetchChunk(chunk *byteChunk) error {
	byteRange := fmt.Sprintf(
		"bytes=%d-%d",
		chunk.start+int64(len(chunk.data)),
		chunk.end-1,
	)
	log.WithFields(log.Fields{
		"range": byteRange,
		"chunk": chunk,
	}).Debug("getting range")
	resp, err := c.bkt.GetResponseWithHeaders(c.key.Key, map[string][]string{
		"Range": {byteRange},
	})
	if err != nil {
		return fmt.Errorf("doing request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent:
		// all good
	default:
		return fmt.Errorf("bad status: %v", resp.Status)
	}
	wantSize := int(chunk.end - chunk.start)
	missing := wantSize - len(chunk.data)
	buf := bytes.NewBuffer(make([]byte, 0, missing))

	log.WithFields(log.Fields{
		"missing": missing,
		"chunk":   chunk,
	}).Debug("copying range")
	_, err = io.Copy(buf, bufio.NewReader(resp.Body))

	chunk.data = append(chunk.data, buf.Bytes()...)

	if err != nil {
		return fmt.Errorf("reading response: %v", err)
	}

	if len(chunk.data) != wantSize {
		return fmt.Errorf("unexpected data size, want %d got %d", wantSize, len(chunk.data))
	}
	chunk.content <- bytes.NewReader(chunk.data)
	return nil
}

func imin(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

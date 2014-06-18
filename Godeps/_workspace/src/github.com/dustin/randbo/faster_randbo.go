package randbo

import (
	"io"
	"math/rand"
	"time"
)

// Randbo creates a stream of non-crypto quality random bytes
type fasterRandbo struct {
	rand.Source
}

// NewFast creates a new random reader with a time source.
func NewFast() io.Reader {
	return NewFrom(rand.NewSource(time.Now().UnixNano()))
}

// NewFastFrom creates a new reader from your own rand.Source
func NewFastFrom(src rand.Source) io.Reader {
	return &fasterRandbo{src}
}

// Read satisfies io.Reader
func (r *fasterRandbo) Read(p []byte) (n int, err error) {
	todo := len(p)
	offset := 0
	for {
		val := int64(r.Int63())
		for i := 0; i < 8; i++ {
			p[offset] = byte(val)
			todo--
			if todo == 0 {
				return len(p), nil
			}
			offset++
			val >>= 8
		}
	}
}

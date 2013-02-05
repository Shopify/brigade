package infinity

import (
	"log"
	"testing"
)

func TestInfinityChannel(t *testing.T) {
	infinite := New()

	infinite.Push(1)
	log.Printf("Finished push")

	val, ok := infinite.Pop().(int)
	if !ok || val != 1 {
		t.Error("failed to pop value from queue")
	}
}

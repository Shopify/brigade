package infinity

import (
	"container/list"
	"log"
)

type Infinity struct {
	input    chan interface{}
	output   chan interface{}
	shutdown chan int
	storage  list.List
}

func New() *Infinity {
	var ret Infinity

	ret.input = make(chan interface{}, 1)
	ret.output = make(chan interface{}, 1)
	ret.shutdown = make(chan int, 1)

	go ret.run()

	return &ret
}

func (i *Infinity) run() {
	for {
		select {
		case value := <-i.input:
			if value == nil {
				i.output <- i.storage.Remove(i.storage.Front())
			} else {
				i.storage.PushBack(value)
			}
		case <-i.shutdown:
			return
		}
	}
}

func (i *Infinity) Push(value interface{}) {
	i.input <- value
}

func (i *Infinity) Pop() interface{} {
	i.input <- nil
	return <-i.output
}

func (i *Infinity) Halt() {
	i.shutdown <- 1
}

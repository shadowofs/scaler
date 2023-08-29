package util

import (
	"sync/atomic"
)

type AtomicInt32 int32

func (i *AtomicInt32) Load() int32 {
	return atomic.LoadInt32((*int32)(i))
}

func (i *AtomicInt32) Add(delta int) {
	atomic.AddInt32((*int32)(i), int32(delta))
}

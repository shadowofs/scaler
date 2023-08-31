package util

import (
	"sync/atomic"
)

type AtomicInt32 struct {
	val int32
	max int32
}

func (i *AtomicInt32) Load() int32 {
	return atomic.LoadInt32(&i.val)
}

func (i *AtomicInt32) Add(delta int) {
	curVal := atomic.AddInt32(&i.val, int32(delta))
	curMax := i.Max()
	if curVal > curMax {
		atomic.CompareAndSwapInt32(&i.max, curMax, curVal)
	}
}

func (i *AtomicInt32) Max() int32 {
	return atomic.LoadInt32(&i.max)
}

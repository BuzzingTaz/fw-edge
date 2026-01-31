package scheduler

import "sync"

// Dummy frame struct
// TODO: Define common frame structure
type Frame struct {
	Data      []byte
	Timestamp int64
}

type RingBuffer struct {
	mu       sync.Mutex
	cond     *sync.Cond
	data     []*Frame
	head     int
	tail     int
	count    int
	capacity int
}

func NewRingBuffer(capacity int) *RingBuffer {
	rb := &RingBuffer{
		data:     make([]*Frame, capacity),
		capacity: capacity,
	}
	rb.cond = sync.NewCond(&rb.mu)
	return rb
}

// Overwrite oldest data
func (r *RingBuffer) Push(f *Frame) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.count == r.capacity {
		r.head = (r.head + 1) % r.capacity
		r.count--
	}

	r.data[r.tail] = f
	r.tail = (r.tail + 1) % r.capacity
	r.count++

	r.cond.Signal()
}

// Pops the newest old data in the buffer
func (r *RingBuffer) Pop() (f *Frame, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.count == 0 {
		return nil, false
	}

	item := r.data[r.head]
	r.head = (r.head + 1) % r.capacity
	r.count--
	return item, true
}

// PopBlock waits until data is available
func (r *RingBuffer) PopBlock() *Frame {
	r.mu.Lock()
	defer r.mu.Unlock()

	for r.count == 0 {
		r.cond.Wait()
	}

	item := r.data[r.head]
	r.head = (r.head + 1) % r.capacity
	r.count--
	return item
}

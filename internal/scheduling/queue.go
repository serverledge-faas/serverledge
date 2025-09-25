package scheduling

import "sync"

type queue interface {
	enqueue(r *scheduledRequest) bool
	dequeue() *scheduledRequest
	front() *scheduledRequest
	len() int
	isEmpty() bool
	isFull() bool
	Lock()
	Unlock()
}

// circularFifoQueue defines a circular queue
type circularFifoQueue struct {
	sync.Mutex
	data     []*scheduledRequest
	capacity int
	head     int
	tail     int
	size     int
}

// newFIFOQueue creates a queue
func newFIFOQueue(n int) queue {
	if n < 1 {
		return nil
	}
	return &circularFifoQueue{
		data:     make([]*scheduledRequest, n),
		capacity: n,
		head:     0,
		tail:     0,
		size:     0,
	}
}

// isEmpty returns true if queue is empty
func (q *circularFifoQueue) isEmpty() bool {
	return q != nil && q.size == 0
}

// isFull returns true if queue is full
func (q *circularFifoQueue) isFull() bool {
	return q.size == q.capacity
}

// Enqueue pushes an element to the back
func (q *circularFifoQueue) enqueue(v *scheduledRequest) bool {
	if q.isFull() {
		return false
	}

	q.data[q.tail] = v
	q.tail = (q.tail + 1) % q.capacity
	q.size = q.size + 1
	return true
}

// Dequeue fetches a element from queue
func (q *circularFifoQueue) dequeue() *scheduledRequest {
	if q.isEmpty() {
		return nil
	}
	v := q.data[q.head]
	q.head = (q.head + 1) % q.capacity
	q.size = q.size - 1
	return v
}

func (q *circularFifoQueue) front() *scheduledRequest {
	if q.isEmpty() {
		return nil
	}
	v := q.data[q.head]
	return v
}

// Len returns the current length of the queue
func (q *circularFifoQueue) len() int {
	return q.size
}

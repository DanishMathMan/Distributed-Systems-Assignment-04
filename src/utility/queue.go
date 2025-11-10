package utility

type Queue struct {
	data []int64 //value corresponds to the id / port of the Node for which a reply must be made to
}

// func for enqueuing into the queue
func (queue *Queue) Enqueue(in int64) {
	queue.data = append(queue.data, in)
}

// func for dequeuing into the queue
func (queue *Queue) Dequeue() int64 {
	dequeued := queue.data[0]
	queue.data = queue.data[1:]
	return dequeued
}

func (queue *Queue) IsEmpty() bool {
	return len(queue.data) == 0
}

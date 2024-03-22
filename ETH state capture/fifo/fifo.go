package fifo

type Node struct {
	value interface{}
	next  *Node
	prev  *Node
}

type FIFO struct {
	head   *Node
	tail   *Node
	size   int
	maxCap int
}

func (f *FIFO) Enqueue(value interface{}) {
	newNode := &Node{value: value}

	if f.head == nil {
		f.head = newNode
		f.tail = newNode
	} else {
		f.tail.next = newNode
		newNode.prev = f.tail
		f.tail = newNode
	}

	f.size++

	if f.size > f.maxCap {
		f.Dequeue()
	}
}

func (f *FIFO) Dequeue() interface{} {
	if f.head == nil {
		return nil
	}

	value := f.head.value
	f.head = f.head.next
	f.size--

	if f.head == nil {
		f.tail = nil
	} else {
		f.head.prev = nil
	}

	return value
}

func (f *FIFO) SetMaxCapacity(capacity int) {
	f.maxCap = capacity

	for f.size > f.maxCap {
		f.Dequeue()
	}
}

func (f *FIFO) GetMaxCapacity() int {
	return f.maxCap
}

func (f *FIFO) GetSize() int {
	return f.size
}

func NewFIFO(maxCapacity int) *FIFO {
	return &FIFO{maxCap: maxCapacity}
}

func (f *FIFO) Traverse(fFunc func(interface{}, int)) {
	i := 0

	for node := f.head; node != nil; node = node.next {
		i++
		fFunc(node.value, i)
	}
}

func (f *FIFO) TraverseReverse(fFunc func(interface{}, int)) {
	i := 0

	for node := f.tail; node != nil; node = node.prev {
		i++
		fFunc(node.value, i)

	}
}

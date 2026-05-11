package scheduler

import (
	"fmt"
	"math/rand/v2"
)

type Nodes []string
type Node string

type Scheduler struct {
	pickNode   func (Nodes, *Frame) Node
	ringBuffer *RingBuffer
	nodes      Nodes
}

func NewScheduler(algorithm func(Nodes, *Frame) Node, ringBuffer *RingBuffer, nodes Nodes) *Scheduler {
	return &Scheduler{
		pickNode:   algorithm,
		ringBuffer: ringBuffer,
		nodes:      nodes,
	}
}

func (s *Scheduler) ScheduleFrame(frame *Frame) {
	s.ringBuffer.Push(frame)
}

func (s *Scheduler) Run() {
	for {
		frame := s.ringBuffer.PopBlock()

		pickedNode := s.pickNode(s.nodes, frame)
		fmt.Println("Picked node:", string(pickedNode))

		// Send Frame
	}
}

func (s *Scheduler) SendFrameToNode(frame *Frame, node Node) {
	// TODO: Implement frame sending logic
	// Random data for now
	println("Sending frame with timestamp", frame.Timestamp, "to node", string(node))
}

func RandomScheduler(nodes []string, frame *Frame) Node{
	// Pick a random node
	n := rand.IntN(len(nodes))

	println("Scheduling frame with timestamp", frame.Timestamp, "to node ", nodes[n])
	return Node(nodes[n])
}

func RRScheduler(nodes []string) func(*Frame) {
	index := 0
	return func(frame *Frame) {
		// Pick node in round-robin fashion
		node := nodes[index]
		index = (index + 1) % len(nodes)

		// Here you would send the frame to the selected node
		// For now, we just print the selected node
		println("Scheduling frame with timestamp", frame.Timestamp, "to node", node)
	}
}

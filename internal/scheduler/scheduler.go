package scheduler

type Scheduler struct {
	pickNode  func(*Frame)
	ringBuffer *RingBuffer
}

func NewScheduler(algorithm func(*Frame), ringBuffer *RingBuffer) *Scheduler {
	return &Scheduler{
		pickNode:  algorithm,
		ringBuffer: ringBuffer,
	}
}

func (s *Scheduler) ScheduleFrame(frame *Frame) {
	s.ringBuffer.Push(frame)
}

func (s *Scheduler) Run() {
	for {
		frame := s.ringBuffer.PopBlock()

		s.pickNode(frame)
	}
}

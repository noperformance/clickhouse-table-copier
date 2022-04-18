package status

import "sync"

type FailType int

const (
	FailReadConfig = 1
	FailCheck      = 1
)

type status struct {
	FinalStatus  int
	DetailStatus map[FailType]bool
}

var (
	once     sync.Once
	instance *status
)

func New() *status {
	once.Do(func() {
		instance = new(status)
		instance.DetailStatus = map[FailType]bool{}
	})
	return instance
}

func (s *status) SetStatus(failType FailType) {
	s.DetailStatus[failType] = true
}

func (s *status) GetFinalStatus() int {
	result := 0
	for k, v := range s.DetailStatus {
		if v {
			result = result | int(k)
		}
	}
	s.FinalStatus = result
	return result
}

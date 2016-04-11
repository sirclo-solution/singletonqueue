package singletonqueue

import (
	"encoding/json"
	"testing"
	"time"
)

type incQueueImpl struct {
	lastValue int
	t         *testing.T
}

func (q *incQueueImpl) GetID() string {
	return "test-ordered-process"
}
func (q *incQueueImpl) Process(message Message) error {
	var value int
	json.Unmarshal(message.Payload, &value)
	if value != q.lastValue+1 {
		q.t.Error("Expected:", q.lastValue+1, "got:", value)
	}
	q.lastValue = value
	return nil
}

func TestOrderedProcess(t *testing.T) {
	incQueue := incQueueImpl{-1, t}

	for i := 0; i < 10; i++ {
		marshaled, _ := json.Marshal(i)
		Push(&incQueue, marshaled)
	}

	startTime := time.Now()
	var err error
	for ret := 1; ret > 0; ret, err = Length(&incQueue) {
		if err != nil {
			t.Error(err)
			return
		}

		if time.Since(startTime) < 3*time.Second {
			time.Sleep(time.Millisecond * 50)
		} else {
			t.Error("Not all jobs processed after 3 seconds")
			return
		}
	}

}

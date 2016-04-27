package singletonqueue

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

const (
	testPrefix = "test-ordered-process"
)

type incQueueImpl struct {
	lastValue int
	t         *testing.T
}

type testMessage struct {
	Value int
}

func (q *incQueueImpl) New(queueID string) Interface {
	if strings.HasPrefix(queueID, testPrefix) {
		newQ := incQueueImpl{-1, q.t}
		return &newQ
	}
	return nil
}

func (q *incQueueImpl) QueueID() string {
	return testPrefix
}

func (q *incQueueImpl) Process(message Message) error {
	var msg testMessage
	json.Unmarshal(message.Payload, &msg)
	if msg.Value != q.lastValue+1 {
		q.t.Error("Expected:", q.lastValue+1, "got:", msg.Value)
	}
	q.lastValue = msg.Value
	return nil
}

func TestOrderedProcess(t *testing.T) {
	incQueue := incQueueImpl{-1, t}

	for i := 0; i < 10; i++ {
		marshaled, _ := json.Marshal(testMessage{
			Value: i,
		})
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

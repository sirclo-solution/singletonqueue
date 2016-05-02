// Package singletonqueue implements global singleton queues of tasks that can be accessed by their IDs.
// Package structure is loosely based on https://golang.org/pkg/container/heap/ .
package singletonqueue

import (
	"github.com/garyburd/redigo/redis"
	log "gopkg.in/Sirupsen/logrus.v0"
	"gopkg.in/vmihailenco/msgpack.v2"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

// RedisPool is the reference to the pool used to draw connections from. Set this before calling any functions in this package.
var RedisPool *redis.Pool

//Interface singleton queue interface
type Interface interface {
	QueueID() string               // Unique ID
	Process(message Message) error // The function that will process messages
	New(queueID string) Interface  // When trying to reinitialize interface given a queueID
}

// Message is passed to Queue.WorkerFunc.
type Message struct {
	Payload []byte
	// TODO: put creation timestamp
}

// RedisQueueKey returns the redis key name of the list used for the queue.
func redisQueueKey(q Interface) string {
	return "sq:q:" + q.QueueID()
}

// RedisLockKey returns the redis key name used to "lock" the worker instance such that only one will run at a time.
func redisLockKey(q Interface) string {
	return "sq:lock:" + q.QueueID()
}

// Length returns the number of jobs currently in the queue.
func Length(q Interface) (int, error) {
	conn := RedisPool.Get()
	defer conn.Close()
	return redis.Int(conn.Do("LLEN", redisQueueKey(q)))
}

// Push queues a new job and runs the worker in a separate goroutine when none is running.
func Push(q Interface, payload []byte) error {
	message := Message{Payload: payload}
	marshaled, err := msgpack.Marshal(message)
	if err != nil {
		return err
	}
	conn := RedisPool.Get()
	defer conn.Close()
	_, err = conn.Do("LPUSH", redisQueueKey(q), marshaled)
	go EnsureWorkerIsRunning(q)
	return err
}

// PushIfEmpty queues a new job only when the queue is empty. Useful for cached values that are refreshed only every so often - this will ensure that there is no dogpile effect (multiple attempts to refresh cache at the same time). Note that the job may still be invoked multiple times in a small amount of time (eg if the second push happens after the worker has done processing the first one), thus the task should be idempotent.
func PushIfEmpty(q Interface, payload []byte) error {
	queueLength, err := Length(q)
	if err != nil {
		return err
	}
	if queueLength == 0 {
		Push(q, payload)
	} else {
		EnsureWorkerIsRunning(q)
	}
	return nil
}

// EnsureWorkerIsRunning runs as a worker if none runs, but returns immediately if one already runs. It is recommended to run this in a goroutine.
func EnsureWorkerIsRunning(q Interface) {
	logger := log.WithField("worker", q.QueueID())
	ret, err := redis.String(safeDo("SET", redisLockKey(q), 1, "NX"))
	if err != nil || ret != "OK" {
		logger.Debug("Unable to acquire lock, another worker is possibly running")
		return
	}

	logger.Info("Worker is starting")
	defer logger.Info("Worker has terminated")
	defer safeDo("DEL", redisLockKey(q))

	quit := make(chan struct{})
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)
	signal.Notify(sigs, syscall.SIGTERM)
	quitting := false

	go func() {
		for {
			select {
			case <-sigs:
				quitting = true
				return
			case <-quit:
				quitting = true
				return
			}
		}
	}()

	for {
		if quitting {
			signal.Stop(sigs)
			close(sigs)
			return
		}
		ret, err := redis.Bytes(safeDo("RPOP", redisQueueKey(q)))
		//log.Info("ret:", ret, " err:", err)
		if err != nil && len(ret) == 0 {
			logger.Debug("No more jobs in the queue, exiting.")
			close(quit)
			return // no more jobs in the queue
		}
		var message Message
		err = msgpack.Unmarshal(ret, &message)
		if err != nil {
			logger.WithError(err).Error("Unmarshal failed, skipping this message")
			// skip this message
			continue
		}

		err = q.Process(message)
		if err != nil {
			logger.Error(err)
			logger.WithError(err).WithField("message", message).Error("Worker returned error")
			// TODO option to retry job if fail
		}
	}

}

func safeDo(commandName string, args ...interface{}) (interface{}, error) {
	conn := RedisPool.Get()
	defer conn.Close()
	return conn.Do(commandName, args...)
}

func Respawn(prefix string, queue Interface) {
	log.Info(prefix)
	ret, _ := redis.Strings(safeDo("KEYS", "sq:q:"+prefix+"*"))

	for _, key := range ret {
		queueID := strings.TrimPrefix(key, "sq:q:")
		newQueue := queue.New(queueID)
		if newQueue != nil {
			go EnsureWorkerIsRunning(newQueue)
		}
	}
}

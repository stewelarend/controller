package controller_test

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stewelarend/controller"
)

func TestWith1Worker2Parts(t *testing.T) {
	//logger.SetGlobalLevel(logger.LevelDebug)
	//100 events in 2 partitions, but only one worker, so will be in order 0,1,2,...,99 all on the same worker
	stream := newStream(100, 2)
	handler := &handler{t: t}
	if err := controller.Run(controller.Config{NrWorkers: 1}, stream, handler); err != nil {
		panic(err)
	}
	t.Logf("Done")
	if handler.count != 100 {
		t.Fatalf("got %d events != %d", handler.count, stream.nrEvents)
	}
	//check that all events were processed
}

func TestWith2Workers2Parts(t *testing.T) {
	//logger.SetGlobalLevel(logger.LevelDebug)
	//100 events in 2 partitions, with 2 workers, will use both workers and even nrs will be in order, and odd nrs will be in order
	//but some odds may before evens and vica versa...
	stream := newStream(100, 2)
	handler := &handler{t: t}
	if err := controller.Run(controller.Config{NrWorkers: 2}, &stream, handler); err != nil {
		panic(err)
	}
	t.Logf("Done")
	if handler.count != 100 {
		t.Fatalf("got %d events != %d", handler.count, stream.nrEvents)
	}
	//check that all events were processed
}

func TestWith100Workers2Parts(t *testing.T) {
	//logger.SetGlobalLevel(logger.LevelDebug)
	//100 events in 2 partitions, with 100 workers, will only use 2 workers and even nrs will be in order, and odd nrs will be in order
	//but some odds may before evens and vica versa...
	stream := newStream(100, 2)
	handler := &handler{t: t}
	if err := controller.Run(controller.Config{NrWorkers: 100}, &stream, handler); err != nil {
		panic(err)
	}
	t.Logf("Done")
	if handler.count != 100 {
		t.Fatalf("got %d events != %d", handler.count, stream.nrEvents)
	}
	//check that all events were processed
}

func TestWith3Workers10Parts(t *testing.T) {
	//logger.SetGlobalLevel(logger.LevelDebug)
	//100 events in 10 partitions, with only 3 workers, will only all 3 workers with multiple partitions on each
	//every 10th event will be in order, i.e. 1, 11, 21, ... will be in order and 2, 12, 22, ... as well,
	//but 2 may be before 1, but 12 will not be before 2...
	stream := newStream(100, 10)
	handler := &handler{t: t}
	if err := controller.Run(controller.Config{NrWorkers: 3}, &stream, handler); err != nil {
		panic(err)
	}
	t.Logf("Done")
	if handler.count != 100 {
		t.Fatalf("got %d events != %d", handler.count, stream.nrEvents)
	}
	//check that all events were processed
}

func newStream(nrEvents int, nrParts int) stream {
	s := stream{
		nrEvents:  nrEvents,
		nrParts:   nrParts,
		next:      0,
		eventChan: make(chan controller.Event),
	}
	go func() {
		for s.next < s.nrEvents {
			value := s.next
			eventData, _ := json.Marshal(map[string]interface{}{
				"type": "task",
				"request": map[string]interface{}{
					"value": value,
				},
			})
			partitionKey := fmt.Sprintf("%08d", value%s.nrParts)
			s.next++
			s.eventChan <- controller.Event{
				Data:         eventData,
				PartitionKey: partitionKey,
				Error:        nil,
			}
		}
		s.eventChan <- controller.Event{
			Data:         nil,
			PartitionKey: "",
			Error:        fmt.Errorf("end of stream"),
		}
	}()
	return s
}

func (s stream) EventChan() <-chan controller.Event {
	return s.eventChan
}

//internal stream to produce events from 0..99
//this simulates a stream like kafka/nats/... even file processing
type stream struct {
	nrEvents  int
	nrParts   int
	next      int
	eventChan chan controller.Event
}

func (s *stream) NextEvent(maxDur time.Duration) (eventData []byte, partitionKey string, err error) {
	if s.next >= s.nrEvents {
		return nil, "", fmt.Errorf("end of stream after %d events", s.nrEvents)
	}
	//simulate a eventData as a network message from some producer...
	value := s.next
	eventData, _ = json.Marshal(map[string]interface{}{
		"type": "task",
		"request": map[string]interface{}{
			"value": value,
		},
	})
	partitionKey = fmt.Sprintf("%08d", value%s.nrParts)
	s.next++
	return eventData, partitionKey, nil
}

type handler struct {
	sync.Mutex
	t     *testing.T
	count int
}

type Message struct {
	Type    string      `json:"type"`
	Request interface{} `json:"request"`
}

//Handle is called in a background worker
//it may panic on error or return error value...
func (h *handler) Handle(ctx controller.Context, eventData []byte) error {
	//parse event data from stream, a JSON message, into our own message struct
	msg := Message{}
	if err := json.Unmarshal(eventData, &msg); err != nil {
		panic(fmt.Errorf("ERROR: cannot parse message as JSON: %v", err))
	}
	//h.t.Logf("msg: %+v", msg)
	h.Lock()
	h.count++
	h.Unlock()
	return nil
}

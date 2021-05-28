package controller

import (
	"context"
	"fmt"
	"time"
)

type worker struct {
	c             *controller
	id            int //0,1,2,...NrWorkers-1
	handler       IEventHandler
	taskChan      chan task
	partitionKeys map[string]bool
}

func (w *worker) run() {
	log = log.New(fmt.Sprintf("worker[%d]", w.id)).With("worker", w.id)
	log.Debugf("worker[%d] starting...", w.id)
	for {
		log.Debugf("worker[%d] checking for task...", w.id)
		var t task
		select {
		case t = <-w.taskChan:
			//immediately got another task, submitted while processing the previous one
			log.Debugf("worker[%d].task[%d].partition(%s) immed-> (%T)%+v", w.id, t.id, t.partitionKey, t.eventData, t.eventData)

		default:
			//no more tasks queued, this worker is now idle and no longer responsible for the partitions it had
			log.Debugf("worker[%d] idle ...", w.id)
			if len(w.partitionKeys) == 0 {
				//first time go idle immediately
				w.c.freeWorkers <- w
			} else {
				//finished processing
				w.c.partitionMutex.Lock()
				for pk := range w.partitionKeys {
					delete(w.c.partitionWorker, pk) //clear each partitionKey associated to this worker
				}
				w.partitionKeys = map[string]bool{} //no more partitionKeys remain to this worker
				w.c.freeWorkers <- w
				w.c.partitionMutex.Unlock()
			}

			//block to wait for next task
			t = <-w.taskChan
			if t.eventData != nil {
				log.Debugf("worker[%d].task[%d].partition(%s) wait -> (%T)%+v", w.id, t.id, t.partitionKey, t.eventData, t.eventData)
			}
		}

		if t.eventData == nil {
			//we get this when controller is shutting down and closed the w.taskChan
			break
		}

		w.partitionKeys[t.partitionKey] = true //add to list that will be cleared when going idle
		log.Debugf("worker[%d] has %d partitions: %+v", w.id, len(w.partitionKeys), w.partitionKeys)

		//process task in a func so we can use defer for returning the tark as to recover from handler panic ...
		func(t task) {
			defer func() {
				if r := recover(); r != nil {
					log.Errorf("event handler panic: %v", r)
				}
				w.c.freeTasks <- t.id //put the task id back when the task is done
			}()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			if err := w.handler.Handle(
				Context{Context: ctx, ILogger: log},
				t.eventData,
			); err != nil {
				log.Errorf("event handler failed: %v", err)
				return
			}
		}(t)
	}
	log.Debugf("terminated.")
} //worker

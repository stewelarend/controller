package controller

import (
	"context"
	"time"

	"github.com/stewelarend/logger"
)

type worker struct {
	//c        *controller
	partition string
	log       logger.ILogger
	handler   IEventHandler
	taskChan  chan task
}

func (w *worker) run(doneTaskChan chan int, terminatedChan chan *worker) {
	for t := range w.taskChan {
		if t.eventData == nil {
			//we get this when controller is shutting down and closed the w.taskChan
			break
		}

		//process task in a func so we can use defer for returning the tark as to recover from handler panic ...
		func(t task) {
			defer func() {
				if r := recover(); r != nil {
					w.log.Errorf("event handler panic: %v", r)
				}
				doneTaskChan <- t.id
			}()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			if err := w.handler.Handle(
				Context{Context: ctx, ILogger: w.log.With("context", true)},
				t.eventData,
			); err != nil {
				w.log.Errorf("event handler failed: %v", err)
				return
			}
		}(t)
	} //for each task
	terminatedChan <- w
} //worker

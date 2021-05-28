package controller

import (
	"fmt"
	"sync"
	"time"

	"github.com/stewelarend/logger"
)

type Config struct {
	NrWorkers int `json:"nr_workers"`
}

func (config Config) Validate() error {
	if config.NrWorkers < 1 {
		return fmt.Errorf("nr_workers:%d must be >0", config.NrWorkers)
	}
	return nil
}

type task struct {
	id           int //0,1,2,...NrWorkers-1
	partitionKey string
	eventData    []byte
}

type doneTask struct {
	id     int //0,1,2,...
	worker *worker
}

func Run(config Config, eventStream IEventStream, eventHandler IEventHandler) (err error) {
	log := logger.New()

	//create free tasks to limit concurrency
	//an event is only taken from the stream when we have one of these available
	freeTasks := make(chan int, config.NrWorkers)
	for i := 0; i < config.NrWorkers; i++ {
		freeTasks <- i
	}
	log.Debugf("Created %d tasks", config.NrWorkers)

	//channel for completed tasks
	//workers put tasks back into this chan then we make them free again
	doneTasks := make(chan int, config.NrWorkers)

	//lookup for existing partition workers to ensure events with the
	//same partition!="" goes to the same worker or a new worker after
	//the previous partition worker terminated, which is fine as well
	//because it keeps the event processing in serie for a partition
	workerMutex := sync.Mutex{}
	partitionWorker := map[string]*worker{}

	//chan for workers that terminated to clean up the partition if they
	//are the last worker created for that partition
	terminatedChan := make(chan *worker)
	go func() {
		for w := range terminatedChan {
			//check if this was the last worker on partition then clear partition
			workerMutex.Lock()
			if partitionWorker[w.partition] == w {
				delete(partitionWorker, w.partition)
			}
			workerMutex.Unlock()
		}
	}()

	//start of main loop
	workerGroup := sync.WaitGroup{}
	for {
		//need a task token to limit concurrency...
		//while waiting, also handle completed tasks to put them back in the free list
		taskID := -1
		for taskID < 0 {
			select {
			case doneTaskID := <-doneTasks:
				freeTasks <- doneTaskID
			case taskID = <-freeTasks:
				log.Debugf("got task")
			}
		}

		//got a free task token, it is later put into completedTask by the worker
		//get the next event from the stream
		//this again is a blocking call until an event is received
		//an error is returned only when the stream broke and it will end the run
		//at the same time, also handle completed tasks
		event := Event{Data: nil, PartitionKey: "", Error: nil}
		for event.Data == nil && event.Error == nil {
			select {
			case doneTaskID := <-doneTasks:
				freeTasks <- doneTaskID
			case event = <-eventStream.EventChan():
				log.Debugf("event: %+v", event)
			}
		}

		if event.Error != nil {
			//cannot get another event, e.g. end of stream
			//put token back and terminate with an error
			freeTasks <- taskID
			err = fmt.Errorf("eventStream broke: %v", event.Error)
			break
		}

		//got an event to process
		//select existing or new partition
		//note all blank partition == "" has a unique concurrent partition, not serialised
		workerMutex.Lock()
		var w *worker
		if event.PartitionKey != "" {
			w, _ = partitionWorker[event.PartitionKey]
		}
		if w == nil {
			//create a new worker
			w = &worker{
				partition: event.PartitionKey,
				log:       log.With("partition", event.PartitionKey),
				handler:   eventHandler,
				taskChan:  make(chan task, config.NrWorkers),
			}
			w.taskChan <- task{id: taskID, eventData: event.Data, partitionKey: event.PartitionKey}

			if event.PartitionKey != "" {
				partitionWorker[event.PartitionKey] = w
				log.Debugf("created partition worker, now %d partitions", len(partitionWorker))
			} else {
				log.Debugf("created once-off worker")
				close(w.taskChan) //once-off only process one task then terminate
			}
			//start the new worker
			workerGroup.Add(1)
			go func(w *worker) {
				w.run(doneTasks, terminatedChan)
				workerGroup.Done()
			}(w)
		} else {
			//existing partition worker
			w.taskChan <- task{id: taskID, eventData: event.Data, partitionKey: event.PartitionKey}
		}
		workerMutex.Unlock()
	} //for main loop

	//out of main loop
	//close taskChan in each worker, which will make the worker complete what its
	//doing then terminate
	log.Debugf("shutdown: closing all remaining partition workers...")
	workerMutex.Lock()
	for _, w := range partitionWorker {
		close(w.taskChan)
	}
	workerMutex.Unlock()

	//take all the tasks from the task channel or any new ones in done chan
	//they will be returned as the workers terminate
	log.Debugf("shutdown: withdrawing %d tasks...", config.NrWorkers)
	remain := config.NrWorkers
	lastReport := time.Now()
	for remain > 0 {
		//to help debug stuck tasks, put a timeout on getting tasks
		//and log remaining workers when excessive time is spent
		select {
		case doneTaskID := <-doneTasks:
			remain--
			log.Debugf("recovered task[%d], %d remain", doneTaskID, remain)
		case taskID := <-freeTasks:
			remain--
			log.Debugf("recovered task[%d], %d remain", taskID, remain)
		case <-time.After(time.Second):
			log.Debugf("Waiting for %d tasks in %d partitions to complete ...", remain, len(partitionWorker))
		}

		if remain > 0 && lastReport.Add(time.Second*10).Before(time.Now()) {
			//waited 10 seconds ... log report
			log.Debugf("Waiting for %d tasks in %d partitions to complete ...", remain, len(partitionWorker))
			for partition, w := range partitionWorker {
				log.Debugf("  partition(%s) still has %d events to process ...", partition, len(w.taskChan))
			}
			lastReport = time.Now()
		}
	}

	//finally wait for all workers to terminate
	//this should be quick as they already returned their tasks
	//and this just wait for them to realise the taskChan is closed
	//and the controller to get all the terminatedChan events
	log.Debugf("waiting for workers")
	workerGroup.Wait()
	close(terminatedChan)
	close(doneTasks)
	close(freeTasks)

	//now ready to terminate
	log.Debugf("shutdown: done")
	return nil
}

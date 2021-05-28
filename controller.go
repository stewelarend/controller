package controller

import (
	"fmt"
	"sync"
	"time"

	"github.com/stewelarend/logger"
)

var log = logger.New()

type Config struct {
	NrWorkers int `json:"nr_workers"`
}

func (config Config) Validate() error {
	if config.NrWorkers < 1 {
		return fmt.Errorf("nr_workers:%d must be >0", config.NrWorkers)
	}
	return nil
}

type controller struct {
	config          Config
	freeTasks       chan int //store task ID 0,1,2,...NrWorkers-1
	freeWorkers     chan *worker
	workers         []*worker
	partitionMutex  sync.Mutex
	partitionWorker map[string]*worker //map[<partitionKey>]<worker index>
}

type task struct {
	id           int //0,1,2,...NrWorkers-1
	partitionKey string
	eventData    []byte
}

func Run(config Config, eventStream IEventStream, eventHandler IEventHandler) (err error) {
	c := controller{
		config: config,
	}

	//prepare tasks and workers
	//+1 workers because task is put back in free list before worker goes idle
	//so there must be one more worker than there are tasks, although a task is
	//needed to start processing, so all N+1 workers won't be used at any point
	//in time, it just ensures we do not dead-lock having a task but still waiting
	//for that worker to go idle at the same time that the worker holds the lock
	//to release it's partition keys...
	c.freeTasks = make(chan int, config.NrWorkers)
	c.freeWorkers = make(chan *worker, config.NrWorkers+1)
	c.workers = make([]*worker, config.NrWorkers+1)
	c.partitionWorker = map[string]*worker{}

	for i := 0; i <= c.config.NrWorkers; i++ { //notice the <= to create NrWorkers+1!!! Not a mistake, do not change it :-)
		//worker.taskChan must worst case be as big as the nr of workers
		//because we can grab that nr of messages with same partition key meaning
		//one worker will get them all
		w := &worker{
			c:             &c,
			id:            i,
			handler:       eventHandler,
			taskChan:      make(chan task, c.config.NrWorkers),
			partitionKeys: map[string]bool{},
		}
		c.workers[i] = w

		//run the worker on that chan until the channel is closed
		//(the worker will put itself in the freeWorkerChan)
		go w.run()

	}
	log.Debugf("Created %d workers", c.config.NrWorkers)

	//create NrWorker tasks
	for i := 0; i < c.config.NrWorkers; i++ {
		c.freeTasks <- i
	}

	//start of main loop
	for {
		//get a task token to limit concurrency...
		//this is a blocking call until we get a token
		//and this token is put back by the worker when it completed the task
		//there is one task per worker, but multiple tasks can be sent to the same worker,
		//based on the partition key to serialise related tasks, in which case
		//some workers will have multiple tasks queued and others will remain idle
		log.Debugf("waiting for free task... (freeTasks:%d, freeWorkers:%d)", len(c.freeTasks), len(c.freeWorkers))
		taskID := <-c.freeTasks
		log.Debugf("got free task[%d]", taskID)

		//get the next event from the stream
		//this again is a blocking call until an event is received
		//an error is returned only when the stream broke and it will
		//end the run
		eventData, partitionKey, err := eventStream.NextEvent(0) //0=block indefinitely
		if err != nil {
			//cannot get another event
			//put token back and terminate with an error
			c.freeTasks <- taskID
			err = fmt.Errorf("eventStream broke: %v", err)
			break
		}

		//got an event to process
		//see if partitionKey is already assigned to a worker else assign next free worker
		c.partitionMutex.Lock()
		var worker *worker
		ok := false
		if partitionKey != "" {
			worker, ok = c.partitionWorker[partitionKey]
		}
		if !ok {
			//no worker yet on this partition key, select a free worker
			//since we have a task token, there should always be a free worker, no blocking
			worker = <-c.freeWorkers
			if partitionKey != "" {
				c.partitionWorker[partitionKey] = worker
			}
		}
		log.Debugf("Sending task[%d].partition(%s)->worker[%d]...", taskID, partitionKey, worker.id)
		worker.taskChan <- task{id: taskID, eventData: eventData, partitionKey: partitionKey}
		c.partitionMutex.Unlock()
		log.Debugf("Sent task[%d].partition(%s)->worker[%d] (now %d in workerTaskChan)", taskID, partitionKey, worker.id, len(worker.taskChan))

		//note: taskToken is not released here, because the worker will put
		//it back when it completed processing the task
	} //for main loop

	//out of main loop
	//close taskChan in each worker, which will make the worker complete what its
	//doing then terminate
	log.Debugf("shutdown: closing worker task channels...")
	for i := 0; i < c.config.NrWorkers; i++ {
		close(c.workers[i].taskChan)
	}

	//take all the tasks from the task channel
	//they will be returned as the workers terminate
	log.Debugf("shutdown: withdrawing %d tasks...", c.config.NrWorkers)
	remain := c.config.NrWorkers
	lastReport := time.Now()
	for remain > 0 {
		//to help debug stuck tasks, put a timeout on getting tasks
		//and log remaining workers when excessive time is spent
		select {
		case <-c.freeTasks:
			remain--
		case <-time.After(time.Second):
			log.Debugf("Waiting for %d tasks in %d partitions to complete ...", remain, len(c.partitionWorker))
		}

		if remain > 0 && lastReport.Add(time.Second*10).Before(time.Now()) {
			//waited 10 second without any tasks completing
			log.Debugf("Waiting for %d tasks in %d partitions to complete ...", remain, len(c.partitionWorker))
			for partitionKey, worker := range c.partitionWorker {
				log.Debugf("  partitionKey(%s).worker[%d] still has %d events to process ...", partitionKey, worker.id, len(worker.taskChan))
			}
			lastReport = time.Now()
		}
	}

	//now ready to terminate
	log.Debugf("shutdown: done")
	return nil
}

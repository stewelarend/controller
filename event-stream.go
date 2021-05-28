package controller

type IEventStream interface {
	//NextEvent return success with an event as: data=[]byte, partitionKey="" or "..." and err=nil
	//if maxDur > 0 and no event by that time, return nil,"",err=nil
	//if closed or cannot get event, return nil,"",err!=nil
	EventChan() <-chan Event
	//NextEvent(maxDir time.Duration) (eventData []byte, partitionKey string, err error)
}

type Event struct {
	Data         []byte
	PartitionKey string
	Error        error
}

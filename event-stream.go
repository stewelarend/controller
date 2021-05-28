package controller

import "time"

type IEventStream interface {
	//NextEvent return success with an event as: data=[]byte, partitionKey="" or "..." and err=nil
	//if maxDur > 0 and no event by that time, return nil,"",err=nil
	//if closed or cannot get event, return nil,"",err!=nil
	NextEvent(maxDir time.Duration) (eventData []byte, partitionKey string, err error)
}

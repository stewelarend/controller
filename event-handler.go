package controller

type IEventHandler interface {
	Handle(ctx Context, eventData []byte) error
}

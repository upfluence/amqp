package backend

import (
	"context"
	"sync"

	ramqp "github.com/rabbitmq/amqp091-go"
)

type channelWrapper struct {
	*ramqp.Channel

	broker    *Broker
	closeOnce sync.Once
}

func (cw *channelWrapper) Open(context.Context) error { return nil }

func (cw *channelWrapper) IsOpen() bool {
	closed := cw.Channel.IsClosed()

	if closed {
		cw.closeOnce.Do(func() {
			cw.broker.idle.Add(-1)
		})
	}

	return !closed
}

func (cw *channelWrapper) Close() error {
	var err error

	cw.closeOnce.Do(func() {
		cw.broker.idle.Add(-1)
		err = cw.Channel.Close()
	})

	return err
}

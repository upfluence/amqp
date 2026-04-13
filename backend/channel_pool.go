package backend

import (
	"context"
	"sync"

	ramqp "github.com/rabbitmq/amqp091-go"
)

// channelWrapper wraps a RabbitMQ channel for use in the channel pool.
//
// It implements the iopool.Resource interface and manages the channel lifecycle,
// ensuring proper cleanup and tracking of idle/used channels.
type channelWrapper struct {
	*ramqp.Channel

	closeOnce sync.Once
}

func (cw *channelWrapper) Open(context.Context) error { return nil }

func (cw *channelWrapper) IsOpen() bool {
	return !cw.Channel.IsClosed()
}

func (cw *channelWrapper) Close() error {
	var err error

	cw.closeOnce.Do(func() {
		err = cw.Channel.Close()
	})

	return err
}

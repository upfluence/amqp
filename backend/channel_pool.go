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

	broker    *Broker
	closeOnce sync.Once
}

// Open is a no-op that satisfies the iopool.Resource interface.
// Channels are already open when created.
func (cw *channelWrapper) Open(context.Context) error { return nil }

// IsOpen returns true if the underlying RabbitMQ channel is still open.
//
// If the channel is closed, it automatically updates the broker's idle channel count.
func (cw *channelWrapper) IsOpen() bool {
	closed := cw.Channel.IsClosed()

	if closed {
		cw.closeOnce.Do(func() {
			cw.broker.idle.Add(-1)
		})
	}

	return !closed
}

// Close closes the channel and updates the broker's idle channel count.
//
// This method is safe to call multiple times; the channel is only closed once.
func (cw *channelWrapper) Close() error {
	var err error

	cw.closeOnce.Do(func() {
		cw.broker.idle.Add(-1)
		err = cw.Channel.Close()
	})

	return err
}

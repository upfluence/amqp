package backend

import (
	"context"
	"io"
	"sync"
	"sync/atomic"

	ramqp "github.com/rabbitmq/amqp091-go"
	"github.com/upfluence/errors"

	"github.com/upfluence/amqp"
)

// ErrConsumerClosed is returned when operations are attempted on a closed consumer.
var ErrConsumerClosed = errors.New("consumer is closed")

// consumer implements the amqp.Consumer interface for RabbitMQ.
// It wraps a channel and manages the lifecycle of message consumption.
type consumer struct {
	broker     *Broker
	consumer   string                // consumer tag (set at construction time)
	channel    *channelWrapper       // dedicated channel for this consumer
	deliveries <-chan ramqp.Delivery // delivery channel from RabbitMQ

	closedOnce sync.Once
	closed     atomic.Bool
}

// Next waits for and returns the next message delivery.
//
// This method blocks until a message is available, the context is cancelled,
// or the consumer is closed. If the delivery channel is closed by the server,
// the consumer is automatically cleaned up.
func (c *consumer) Next(ctx context.Context) (*amqp.Delivery, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case d, ok := <-c.deliveries:
		if !ok {
			return nil, c.cleanup(io.EOF)
		}

		return &amqp.Delivery{
			Message: amqp.Message{
				Body:            d.Body,
				Headers:         d.Headers,
				ContentType:     d.ContentType,
				ContentEncoding: d.ContentEncoding,
				DeliveryMode:    d.DeliveryMode,
				Priority:        d.Priority,
				CorrelationID:   d.CorrelationId,
				ReplyTo:         d.ReplyTo,
				Expiration:      d.Expiration,
				MessageID:       d.MessageId,
				Timestamp:       d.Timestamp,
				Type:            d.Type,
				UserID:          d.UserId,
				AppID:           d.AppId,
			},
			DeliveryTag: d.DeliveryTag,
		}, nil
	}
}

// IsOpen returns true if the consumer is still active and can receive messages.
//
// A consumer becomes inactive when it's explicitly closed or when the underlying
// channel is closed by the server (e.g., due to connection loss).
func (c *consumer) IsOpen() bool {
	if c.closed.Load() {
		return false
	}

	return c.channel.IsOpen()
}

// Ack positively acknowledges a message delivery.
//
// The message will not be redelivered and is removed from the queue.
// If opts.Multiple is true, all messages up to and including the tag are acknowledged.
func (c *consumer) Ack(_ context.Context, tag uint64, opts amqp.AckOptions) error {
	return c.channel.Ack(tag, opts.Multiple)
}

// Nack negatively acknowledges a message delivery.
//
// If opts.Requeue is true, the message is requeued for redelivery.
// If false, the message is discarded or sent to the dead letter exchange if configured.
// If opts.Multiple is true, all messages up to and including the tag are nacked.
func (c *consumer) Nack(_ context.Context, tag uint64, opts amqp.NackOptions) error {
	return c.channel.Nack(tag, opts.Multiple, opts.Requeue)
}

// cleanup handles consumer shutdown and resource cleanup.
//
// If inboundError is nil, the consumer is cancelled gracefully.
// If inboundError is not nil, the channel is discarded (not returned to pool).
// This ensures that a channel with an error is not reused.
//
// Calling cleanup more than once is safe; subsequent calls are no-ops and
// return nil.
func (c *consumer) cleanup(inboundError error) error {
	var err error

	c.closedOnce.Do(func() {
		c.closed.Store(true)

		if inboundError == nil {
			err = c.channel.Cancel(c.consumer, false)
		}

		if inboundError != nil || err != nil {
			c.broker.discardChannel(c.channel)

			err = inboundError
		} else {
			c.broker.putChannel(c.channel)
		}
	})

	return err
}

// Close closes the consumer and releases its channel back to the pool.
//
// This should be called when the consumer is no longer needed.
// After calling Close, no further operations should be performed on the consumer.
func (c *consumer) Close() error {
	return c.cleanup(nil)
}

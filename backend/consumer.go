package backend

import (
	"context"
	"io"
	"sync"

	ramqp "github.com/rabbitmq/amqp091-go"
	"github.com/upfluence/errors"

	"github.com/upfluence/amqp"
)

var ErrConsumerClosed = errors.New("consumer is closed")

type consumer struct {
	broker     *Broker
	consumer   string
	channel    *channelWrapper
	deliveries <-chan ramqp.Delivery

	closedOnce sync.Once
	closed     bool
}

func (c *consumer) Next(ctx context.Context) (*amqp.Delivery, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case d, ok := <-c.deliveries:
		if !ok {
			return nil, c.cleanup(io.EOF)
		}

		if c.consumer == "" {
			c.consumer = d.ConsumerTag
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

func (c *consumer) IsOpen() bool {
	if c.closed {
		return false
	}

	return c.channel.IsOpen()
}

func (c *consumer) Ack(_ context.Context, tag uint64, opts amqp.AckOptions) error {
	return c.channel.Ack(tag, opts.Multiple)
}

func (c *consumer) Nack(_ context.Context, tag uint64, opts amqp.NackOptions) error {
	return c.channel.Nack(tag, opts.Multiple, opts.Requeue)
}

func (c *consumer) cleanup(inboundError error) error {
	var err error = ErrConsumerClosed

	c.closedOnce.Do(func() {
		c.closed = true
		c.broker.consuming.Add(-1)

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

func (c *consumer) Close() error {
	return c.cleanup(nil)
}

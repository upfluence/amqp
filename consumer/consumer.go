// Package consumer provides utilities for managing AMQP consumers with pooling and anonymous queues.
package consumer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"

	"github.com/upfluence/errors"
	"github.com/upfluence/pkg/v2/iopool"
	"github.com/upfluence/pkg/v2/pool"

	"github.com/upfluence/amqp"
)

// Consumer extends the amqp.Consumer interface with a QueueName method.
type Consumer interface {
	amqp.Consumer

	// QueueName returns the name of the queue this consumer is consuming from.
	QueueName() string
}

// consumer is the internal implementation of Consumer.
type consumer struct {
	amqp.Consumer

	queueName string
}

// Open is a no-op that satisfies the iopool.Resource interface.
func (c *consumer) Open(context.Context) error {
	return nil
}

// QueueName returns the name of the queue this consumer is consuming from.
func (c *consumer) QueueName() string {
	return c.queueName
}

// Pool manages a pool of consumers for efficient resource usage.
type Pool = pool.Pool[Consumer]

// NewConsumerPool creates a new consumer pool.
func NewConsumerPool(b amqp.Broker, opts amqp.ConsumeOptions, popts ...iopool.Option) Pool {
	return pool.NewTransformPool(
		iopool.NewPool(
			func(ctx context.Context) (*consumer, error) {
				return buildConsumer(ctx, b, opts)
			},
			popts...,
		),
		func(cons *consumer) Consumer { return cons },
		func(c Consumer) *consumer { return c.(*consumer) },
	)
}

// BuildConsumer creates a consumer with an automatically generated queue name.
func BuildConsumer(ctx context.Context, b amqp.Broker, opts amqp.ConsumeOptions) (Consumer, error) {
	return buildConsumer(ctx, b, opts)
}

func buildConsumer(ctx context.Context, b amqp.Broker, opts amqp.ConsumeOptions) (*consumer, error) {
	if ca, ok := b.(amqp.AnonymousConsumer); ok {
		qName, cons, err := ca.ConsumeAnonymous(ctx, opts)

		if err != nil {
			return nil, errors.Wrap(err, "failed to start anonymous consumer")
		}

		return &consumer{
			Consumer:  cons,
			queueName: qName,
		}, nil
	}

	buf := make([]byte, 64)

	if _, err := rand.Read(buf); err != nil {
		return nil, errors.Wrap(err, "failed to generate random queue name")
	}

	qName := "consumer." + strings.ToLower(hex.EncodeToString(buf))

	if err := b.DeclareQueue(
		ctx,
		qName,
		amqp.DeclareQueueOptions{AutoDelete: true, Exclusive: true},
	); err != nil {
		return nil, errors.Wrap(err, "failed to declare queue for consumer")
	}

	cons, err := b.Consume(ctx, qName, opts)

	if err != nil {
		return nil, errors.Wrap(err, "failed to start consumer")
	}

	return &consumer{Consumer: cons, queueName: qName}, nil
}

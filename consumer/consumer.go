package consumer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"

	"github.com/upfluence/amqp"
	"github.com/upfluence/errors"
	"github.com/upfluence/pkg/v2/iopool"
)

var errInvalidConsumerType = errors.New("invalid consumer type")

type Consumer interface {
	amqp.Consumer

	QueueName() string
}

type consumer struct {
	amqp.Consumer

	queueName string
}

func (c *consumer) Open(context.Context) error {
	return nil
}

func (c *consumer) QueueName() string {
	return c.queueName
}

type ConsumerPool interface {
	Get(context.Context) (Consumer, error)
	Put(context.Context, Consumer) error
	Discard(context.Context, Consumer) error
	Close() error
}

type consumerPool struct {
	*iopool.Pool[*consumer]
}

func (cp *consumerPool) Get(ctx context.Context) (Consumer, error) { return cp.Pool.Get(ctx) }

func (cp *consumerPool) Put(ctx context.Context, c Consumer) error {
	cons, ok := c.(*consumer)

	if !ok {
		return errInvalidConsumerType
	}

	return cp.Pool.Put(cons)
}

func (cp *consumerPool) Discard(ctx context.Context, c Consumer) error {
	cons, ok := c.(*consumer)

	if !ok {
		return errInvalidConsumerType
	}

	return cp.Pool.Discard(cons)
}

func NewConsumerPool(b amqp.Broker, opts amqp.ConsumeOptions, popts ...iopool.Option) ConsumerPool {
	return &consumerPool{
		Pool: iopool.NewPool(
			func(ctx context.Context) (*consumer, error) {
				return buildConsumer(ctx, b, opts)
			},
			popts...,
		),
	}
}

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
		amqp.DeclareQueueOptions{AutoDelete: true},
	); err != nil {
		return nil, errors.Wrap(err, "failed to declare queue for consumer")
	}

	cons, err := b.Consume(ctx, qName, opts)

	if err != nil {
		return nil, errors.Wrap(err, "failed to start consumer")
	}

	return &consumer{Consumer: cons, queueName: qName}, nil
}

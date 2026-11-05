package balancer

import (
	"context"
	"net"
	"strconv"
	"sync"

	"github.com/upfluence/errors"
	"github.com/upfluence/pkg/v2/discovery/balancer"
	"github.com/upfluence/pkg/v2/discovery/balancer/simple"
	"github.com/upfluence/pkg/v2/discovery/resolver/static"

	"github.com/upfluence/amqp"
	"github.com/upfluence/amqp/backend/rabbitmq"
	aconsumer "github.com/upfluence/amqp/consumer"
)

type BrokerWrapper struct {
	Broker amqp.Broker
	Host   string
	Port   int
}

func (bw BrokerWrapper) Addr() string {
	if bw.Port == 0 {
		return bw.Host
	}

	return net.JoinHostPort(bw.Host, strconv.Itoa(bw.Port))
}

type Balancer = balancer.Balancer[BrokerWrapper]

// Broker balances operations across several brokers.
//
// Exclusive queues are bound to the connection that declared them, so once
// an exclusive queue is declared, every later operation on it is routed to
// the same underlying broker for as long as its connection stays open.
type Broker struct {
	b    Balancer
	opts balancer.GetOptions

	mu     sync.Mutex
	pinned map[string]amqp.Broker
}

func NewBroker(b Balancer, opts balancer.GetOptions) *Broker {
	return &Broker{b: b, opts: opts, pinned: make(map[string]amqp.Broker)}
}

func NewDefaultBroker(bs ...BrokerWrapper) *Broker {
	r := static.NewResolver(bs)
	b := balancer.PolicyBalancerFunc(simple.NewPolicy(&Picker{}))(r)

	return NewBroker(b, balancer.GetOptions{})
}

func (b *Broker) Close() error {
	return b.b.Close()
}

func (b *Broker) withBroker(ctx context.Context, fn func(amqp.Broker) error) error {
	pb, done, err := b.b.Get(ctx, b.opts)

	if err != nil {
		return err
	}

	err = fn(pb.Broker)

	done(err)

	return err
}

func (b *Broker) pinnedBroker(queue string) (amqp.Broker, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	pb, ok := b.pinned[queue]

	if !ok {
		return nil, false
	}

	// The exclusive queue died with the connection, any broker can now
	// declare it again.
	if !rabbitmq.GetStats(pb).ConnectionOpened {
		delete(b.pinned, queue)

		return nil, false
	}

	return pb, true
}

func (b *Broker) pin(queue string, pb amqp.Broker) {
	b.mu.Lock()
	b.pinned[queue] = pb
	b.mu.Unlock()
}

func (b *Broker) withQueueBroker(ctx context.Context, queue string, fn func(amqp.Broker) error) error {
	if pb, ok := b.pinnedBroker(queue); ok {
		return fn(pb)
	}

	return b.withBroker(ctx, fn)
}

func (b *Broker) Publish(ctx context.Context, exchange, routingKey string, msg amqp.Message, opts amqp.PublishOptions) error {
	return b.withBroker(ctx, func(b amqp.Broker) error {
		return b.Publish(ctx, exchange, routingKey, msg, opts)
	})
}

func (b *Broker) Consume(ctx context.Context, queue string, opts amqp.ConsumeOptions) (amqp.Consumer, error) {
	if pb, ok := b.pinnedBroker(queue); ok {
		c, err := pb.Consume(ctx, queue, opts)

		return c, errors.WithStack(err)
	}

	pb, done, err := b.b.Get(ctx, b.opts)

	if err != nil {
		return nil, err
	}

	c, err := pb.Broker.Consume(ctx, queue, opts)

	if err != nil {
		done(err)

		return nil, err
	}

	return newConsumer(c, done), nil
}

func (b *Broker) ConsumeAnonymous(ctx context.Context, opts amqp.ConsumeOptions) (string, amqp.Consumer, error) {
	pb, done, err := b.b.Get(ctx, b.opts)

	if err != nil {
		return "", nil, err
	}

	ac, ok := pb.Broker.(amqp.AnonymousConsumer)

	if !ok {
		c, err := aconsumer.BuildConsumer(ctx, pb.Broker, opts)

		if err != nil {
			done(err)

			return "", nil, err
		}

		return c.QueueName(), newConsumer(c, done), nil
	}

	queue, c, err := ac.ConsumeAnonymous(ctx, opts)

	if err != nil {
		done(err)

		return "", nil, err
	}

	return queue, newConsumer(c, done), nil
}

func (b *Broker) DeclareQueue(ctx context.Context, name string, opts amqp.DeclareQueueOptions) error {
	return b.withQueueBroker(ctx, name, func(pb amqp.Broker) error {
		if err := pb.DeclareQueue(ctx, name, opts); err != nil {
			return errors.WithStack(err)
		}

		if opts.Exclusive {
			b.pin(name, pb)
		}

		return nil
	})
}

func (b *Broker) DeclareExchange(ctx context.Context, name string, kind amqp.ExchangeKind, opts amqp.DeclareExchangeOptions) error {
	return b.withBroker(ctx, func(b amqp.Broker) error {
		return b.DeclareExchange(ctx, name, kind, opts)
	})
}

func (b *Broker) BindQueue(ctx context.Context, queue, key, exchange string, opts amqp.BindQueueOptions) error {
	return b.withQueueBroker(ctx, queue, func(b amqp.Broker) error {
		return b.BindQueue(ctx, queue, key, exchange, opts)
	})
}

type consumer struct {
	c  amqp.Consumer
	fn func(error)
}

func newConsumer(c amqp.Consumer, done func(error)) *consumer {
	var once sync.Once

	return &consumer{
		c: c,
		fn: func(err error) {
			once.Do(func() { done(err) })
		},
	}
}

func (c *consumer) IsOpen() bool {
	return c.c.IsOpen()
}

func (c *consumer) Close() error {
	err := c.c.Close()

	c.fn(err)

	return err
}

func (c *consumer) Next(ctx context.Context) (*amqp.Delivery, error) {
	d, err := c.c.Next(ctx)

	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		c.fn(err)
	}

	return d, err
}

func (c *consumer) Ack(ctx context.Context, tag uint64, opts amqp.AckOptions) error {
	err := c.c.Ack(ctx, tag, opts)

	if err != nil {
		c.fn(err)
	}

	return err
}

func (c *consumer) Nack(ctx context.Context, tag uint64, opts amqp.NackOptions) error {
	err := c.c.Nack(ctx, tag, opts)

	if err != nil {
		c.fn(err)
	}

	return err
}

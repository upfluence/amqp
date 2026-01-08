package backend

import (
	"context"
	"net"
	"sync/atomic"
	"time"

	ramqp "github.com/rabbitmq/amqp091-go"
	"github.com/upfluence/amqp"
	"github.com/upfluence/errors"
	"github.com/upfluence/pkg/v2/iopool"
	"github.com/upfluence/pkg/v2/limiter"
	"github.com/upfluence/pkg/v2/limiter/rate"
	"github.com/upfluence/pkg/v2/syncutil"
)

type options struct {
	channelPoolOptions []iopool.Option

	limiter limiter.Limiter
	dialer  *net.Dialer

	configOptions []func(*ramqp.Config)
}

func (o *options) amqpConfig() ramqp.Config {
	var cfg ramqp.Config

	for _, opt := range o.configOptions {
		opt(&cfg)
	}

	return cfg
}

type Option func(*options)

func WithProperties(props map[string]interface{}) Option {
	return func(o *options) {
		o.configOptions = append(o.configOptions, func(c *ramqp.Config) {
			if c.Properties == nil {
				c.Properties = make(map[string]interface{})
			}

			for k, v := range props {
				c.Properties[k] = v
			}
		})
	}
}

type BrokerStats struct {
	ConnectionOpened bool

	IdleChannel      int
	InUseChannel     int
	ConsumingChannel int
}

type Broker struct {
	conn *ramqp.Connection
	uri  string
	cfg  ramqp.Config
	sf   syncutil.Singleflight[*ramqp.Connection]

	limiter limiter.Limiter
	dialer  *net.Dialer

	idle      atomic.Int64
	used      atomic.Int64
	consuming atomic.Int64

	channelPool *iopool.Pool[*channelWrapper]
}

func NewBroker(uri string, opts ...Option) *Broker {
	var o = options{
		channelPoolOptions: []iopool.Option{iopool.WithSize(2048), iopool.WithMaxIdle(16)},
		limiter:            rate.NewLimiter(rate.Config{Baseline: 1, Period: time.Second}),
		dialer:             &net.Dialer{Timeout: 30 * time.Second},
	}

	for _, opt := range opts {
		opt(&o)
	}

	b := Broker{
		uri:     uri,
		cfg:     o.amqpConfig(),
		limiter: o.limiter,
		dialer:  o.dialer,
	}

	b.channelPool = iopool.NewPool(b.buildChannel, o.channelPoolOptions...)

	return &b
}

func (b *Broker) Stats() BrokerStats {
	return BrokerStats{
		ConnectionOpened: b.conn != nil && !b.conn.IsClosed(),
		IdleChannel:      int(b.idle.Load()),
		InUseChannel:     int(b.used.Load()),
		ConsumingChannel: int(b.consuming.Load()),
	}
}

func (b *Broker) buildChannel(ctx context.Context) (*channelWrapper, error) {
	conn, err := b.getConn(ctx)

	if err != nil {
		return nil, errors.Wrap(err, "failed to get connection")
	}

	ch, err := conn.Channel()

	if err != nil {
		return nil, errors.Wrap(err, "failed to create channel")
	}

	b.idle.Add(1)

	return &channelWrapper{Channel: ch, broker: b}, nil
}

func (b *Broker) getConn(ctx context.Context) (*ramqp.Connection, error) {
	if b.conn != nil && !b.conn.IsClosed() {
		return b.conn, nil
	}

	_, conn, err := b.sf.Do(ctx, func(ctx context.Context) (*ramqp.Connection, error) {
		if b.conn != nil && !b.conn.IsClosed() {
			return b.conn, nil
		}

		done, err := b.limiter.Allow(ctx, limiter.AllowOptions{})

		if err != nil {
			return nil, errors.Wrap(err, "failed to get rate limiter token")
		}

		defer done()

		c := b.cfg

		c.Dial = func(network, addr string) (net.Conn, error) {
			return b.dialer.DialContext(ctx, network, addr)
		}

		conn, err := ramqp.DialConfig(b.uri, c)

		if err != nil {
			return nil, errors.Wrap(err, "failed to connect to AMQP server")
		}

		b.conn = conn

		return b.conn, nil
	})

	return conn, err
}

func (b *Broker) Close() error {
	err := b.channelPool.Close()

	if !b.conn.IsClosed() {
		err = errors.Combine(err, b.conn.Close())
	}

	return err

}

func (b *Broker) Publish(ctx context.Context, exchange, key string, msg amqp.Message, opts amqp.PublishOptions) error {
	return b.execute(ctx, func(ch *ramqp.Channel) error {
		return ch.PublishWithContext(
			ctx,
			exchange,
			key,
			opts.Mandatory,
			opts.Immediate,
			ramqp.Publishing{
				Headers:         msg.Headers,
				ContentType:     msg.ContentType,
				ContentEncoding: msg.ContentEncoding,
				Body:            msg.Body,
				DeliveryMode:    msg.DeliveryMode,
				Priority:        msg.Priority,
				CorrelationId:   msg.CorrelationID,
				ReplyTo:         msg.ReplyTo,
				Expiration:      msg.Expiration,
				MessageId:       msg.MessageID,
				Timestamp:       msg.Timestamp,
				Type:            msg.Type,
				UserId:          msg.UserID,
				AppId:           msg.AppID,
			},
		)
	})
}

func (b *Broker) getChannel(ctx context.Context) (*channelWrapper, error) {
	ch, err := b.channelPool.Get(ctx)

	if err != nil {
		return nil, errors.Wrap(err, "failed to get channel from pool")
	}

	b.idle.Add(-1)
	b.used.Add(1)

	return ch, nil
}

func (b *Broker) putChannel(ch *channelWrapper) error {
	err := b.channelPool.Put(ch)

	b.idle.Add(1)
	b.used.Add(-1)

	return errors.Wrap(err, "failed to put channel back to pool")
}

func (b *Broker) discardChannel(ch *channelWrapper) error {
	err := b.channelPool.Discard(ch)

	b.idle.Add(1)
	b.used.Add(-1)

	return errors.Wrap(err, "failed to discard channel")
}

func (b *Broker) ConsumeAnonymous(ctx context.Context, opts amqp.ConsumeOptions) (string, amqp.Consumer, error) {
	ch, err := b.getChannel(ctx)

	if err != nil {
		return "", nil, err
	}

	q, err := ch.QueueDeclare("", false, true, true, false, nil)

	if err != nil {
		b.discardChannel(ch)

		return "", nil, errors.Wrap(err, "failed to start consuming from queue")
	}

	deliveries, err := ch.ConsumeWithContext(
		ctx,
		q.Name,
		opts.Consumer,
		opts.AutoACK,
		opts.Exclusive,
		false, // noLocal not supported by RabbitMQ
		false,
		opts.Args,
	)

	if err != nil {
		b.discardChannel(ch)

		return "", nil, errors.Wrap(err, "failed to start consuming from queue")
	}

	b.consuming.Add(1)

	return q.Name, &consumer{
		broker:     b,
		consumer:   opts.Consumer,
		channel:    ch,
		deliveries: deliveries,
	}, nil
}

func (b *Broker) Consume(ctx context.Context, queue string, opts amqp.ConsumeOptions) (amqp.Consumer, error) {
	ch, err := b.getChannel(ctx)

	if err != nil {
		return nil, err
	}

	deliveries, err := ch.ConsumeWithContext(
		ctx,
		queue,
		opts.Consumer,
		opts.AutoACK,
		opts.Exclusive,
		false, // noLocal not supported by RabbitMQ
		false,
		opts.Args,
	)

	if err != nil {
		b.discardChannel(ch)

		return nil, errors.Wrap(err, "failed to start consuming from queue")
	}

	b.consuming.Add(1)

	return &consumer{
		broker:     b,
		consumer:   opts.Consumer,
		channel:    ch,
		deliveries: deliveries,
	}, nil
}

func (b *Broker) execute(ctx context.Context, fn func(*ramqp.Channel) error) error {
	ch, err := b.getChannel(ctx)

	if err != nil {
		return err
	}

	if err := fn(ch.Channel); err != nil {
		b.discardChannel(ch)

		return errors.Wrap(err, "failed to execute function on channel")
	}

	return b.putChannel(ch)
}

func (b *Broker) Qos(ctx context.Context, opts amqp.QosOptions) error {
	return b.execute(ctx, func(ch *ramqp.Channel) error {
		return ch.Qos(opts.PrefetchCount, opts.PrefetchSize, opts.Global)
	})
}

func (b *Broker) DeclareQueue(ctx context.Context, queue string, opts amqp.DeclareQueueOptions) error {
	return b.execute(ctx, func(ch *ramqp.Channel) error {
		_, err := ch.QueueDeclare(
			queue,
			opts.Durable,
			opts.AutoDelete,
			false,
			false,
			opts.Args,
		)

		return err
	})
}

func (b *Broker) DeclareExchange(ctx context.Context, ex string, kind amqp.ExchangeKind, opts amqp.DeclareExchangeOptions) error {
	return b.execute(ctx, func(ch *ramqp.Channel) error {
		return ch.ExchangeDeclare(
			ex,
			string(kind),
			opts.Durable,
			opts.AutoDelete,
			false,
			false,
			opts.Args,
		)
	})
}

func (b *Broker) BindQueue(ctx context.Context, queue, key, exchange string, opts amqp.BindQueueOptions) error {
	return b.execute(context.Background(), func(ch *ramqp.Channel) error {
		return ch.QueueBind(
			queue,
			key,
			exchange,
			false,
			opts.Args,
		)
	})
}

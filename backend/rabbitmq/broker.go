// Package rabbitmq provides the RabbitMQ implementation of the amqp.Broker interface.
//
// This package implements connection management, channel pooling, and automatic reconnection
// for RabbitMQ servers using the AMQP 0.9.1 protocol.
package rabbitmq

import (
	"context"
	"maps"
	"net"
	"sync/atomic"
	"time"

	ramqp "github.com/rabbitmq/amqp091-go"
	"github.com/upfluence/errors"
	"github.com/upfluence/pkg/v2/iopool"
	"github.com/upfluence/pkg/v2/limiter"
	"github.com/upfluence/pkg/v2/limiter/rate"
	"github.com/upfluence/pkg/v2/log"
	"github.com/upfluence/pkg/v2/syncutil"

	"github.com/upfluence/amqp"
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

// Option configures a Broker.
type Option func(*options)

// WithProperties sets connection properties that are sent to the AMQP server during connection.
func WithProperties(props map[string]any) Option {
	return func(o *options) {
		o.configOptions = append(o.configOptions, func(c *ramqp.Config) {
			if c.Properties == nil {
				c.Properties = make(map[string]any)
			}

			maps.Copy(c.Properties, props)
		})
	}
}

// BrokerStats contains statistics about the broker's connection and channel usage.
type BrokerStats struct {
	// ConnectionOpened indicates whether the connection to the AMQP server is currently open.
	ConnectionOpened bool

	// IdleChannel is the number of channels in the pool that are idle and ready for use.
	IdleChannel int

	// InUseChannel is the number of channels currently in use for operations.
	InUseChannel int

	// ConsumingChannel is the number of channels actively consuming messages.
	ConsumingChannel int
}

// Broker is the RabbitMQ implementation of the amqp.Broker interface.
type Broker struct {
	conn atomic.Pointer[ramqp.Connection]
	uri  string
	cfg  ramqp.Config
	sf   syncutil.Singleflight[*ramqp.Connection]

	limiter limiter.Limiter
	dialer  *net.Dialer

	consuming atomic.Int64

	channelPool *iopool.Pool[*channelWrapper]
}

// NewBroker creates a new Broker connected to the specified AMQP URI.
//
// The URI format is: amqp://username:password@host:port/vhost
//
// The broker does not connect immediately; the connection is established
// lazily on the first operation.
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

// Stats returns current statistics about the broker's connection and channel usage.
func (b *Broker) Stats() BrokerStats {
	stats := b.channelPool.Stats()
	conn := b.conn.Load()

	return BrokerStats{
		ConnectionOpened: conn != nil && !conn.IsClosed(),
		IdleChannel:      int(stats.Idle),
		InUseChannel:     int(stats.InUse),
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

	return &channelWrapper{Channel: ch}, nil
}

func (b *Broker) getConn(ctx context.Context) (*ramqp.Connection, error) {
	if conn := b.conn.Load(); conn != nil && !conn.IsClosed() {
		return conn, nil
	}

	_, conn, err := b.sf.Do(ctx, func(ctx context.Context) (*ramqp.Connection, error) {
		if conn := b.conn.Load(); conn != nil && !conn.IsClosed() {
			return conn, nil
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

		b.conn.Store(conn)

		return conn, nil
	})

	return conn, err
}

// Close closes the broker, releasing all channels and the connection.
func (b *Broker) Close() error {
	err := b.channelPool.Close()

	if conn := b.conn.Load(); conn != nil && !conn.IsClosed() {
		err = errors.Combine(err, conn.Close())
	}

	return err
}

// Publish sends a message to an exchange with a routing key.
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

	return ch, nil
}

func (b *Broker) putChannel(ch *channelWrapper) error {
	err := b.channelPool.Put(ch)

	return errors.Wrap(err, "failed to put channel back to pool")
}

func (b *Broker) discardChannel(ch *channelWrapper) {
	if err := b.channelPool.Discard(ch); err != nil {
		log.WithError(err).Warning("failed to discard channel")
	}
}

// ConsumeAnonymous creates a temporary queue with an auto-generated name and starts consuming.
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

// Consume starts consuming messages from a queue.
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

// Qos sets Quality of Service parameters for message delivery.
//
// Note: when opts.Global is false, QoS is applied only to the transient pool
// channel used for this call and has no effect on existing consumer channels.
// Pass opts.Global=true to apply the limit across the whole connection.
func (b *Broker) Qos(ctx context.Context, opts amqp.QosOptions) error {
	return b.execute(ctx, func(ch *ramqp.Channel) error {
		return ch.Qos(opts.PrefetchCount, opts.PrefetchSize, opts.Global)
	})
}

// DeclareQueue creates a queue or verifies that it exists with the given parameters.
// If opts.Passive is true, the server only checks whether the queue exists
// (returning an error if it does not) without creating or modifying it.
func (b *Broker) DeclareQueue(ctx context.Context, queue string, opts amqp.DeclareQueueOptions) error {
	return b.execute(ctx, func(ch *ramqp.Channel) error {
		if opts.Passive {
			_, err := ch.QueueDeclarePassive(queue, opts.Durable, opts.AutoDelete, false, false, opts.Args)

			return err
		}

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

// DeclareExchange creates an exchange or verifies that it exists with the given parameters.
// If opts.Passive is true, the server only checks whether the exchange exists
// (returning an error if it does not) without creating or modifying it.
func (b *Broker) DeclareExchange(ctx context.Context, ex string, kind amqp.ExchangeKind, opts amqp.DeclareExchangeOptions) error {
	return b.execute(ctx, func(ch *ramqp.Channel) error {
		if opts.Passive {
			return ch.ExchangeDeclarePassive(ex, string(kind), opts.Durable, opts.AutoDelete, false, false, opts.Args)
		}

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

// BindQueue creates a binding between a queue and an exchange with a routing key.
func (b *Broker) BindQueue(ctx context.Context, queue, key, exchange string, opts amqp.BindQueueOptions) error {
	return b.execute(ctx, func(ch *ramqp.Channel) error {
		return ch.QueueBind(
			queue,
			key,
			exchange,
			false,
			opts.Args,
		)
	})
}

func GetStats(b amqp.Broker) BrokerStats {
	if ub, ok := b.(interface{ Unwrap() amqp.Broker }); ok {
		return GetStats(ub.Unwrap())
	}

	if sb, ok := b.(interface{ Stats() BrokerStats }); ok {
		return sb.Stats()
	}

	return BrokerStats{}
}

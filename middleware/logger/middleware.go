// Package logger provides logging middleware for AMQP brokers.
package logger

import (
	"context"
	"time"

	"github.com/upfluence/log"
	"github.com/upfluence/log/record"

	"github.com/upfluence/amqp"
)

// Logger is the interface for logging broker operations.
type Logger interface {
	// Log records a broker operation with its result, duration, and optional fields.
	Log(string, error, time.Duration, ...record.Field)
}

// simplifiedLogger adapts a log.Logger to the Logger interface.
type simplifiedLogger struct {
	level  record.Level
	logger log.Logger
}

// Log logs an operation at the configured level with duration and optional fields.
func (l *simplifiedLogger) Log(operation string, err error, d time.Duration, ofs ...record.Field) {
	logger := l.logger.WithFields(log.Field("duration", d))

	if len(ofs) > 0 {
		logger = logger.WithFields(ofs...)
	}

	if err != nil {
		logger = logger.WithError(err)
	}

	logger.Log(l.level, operation)
}

// NewFactory creates a middleware factory with a custom logger implementation.
func NewFactory(l Logger) amqp.MiddlewareFactory {
	return &factory{l: l}
}

// NewLevelFactory creates a middleware factory that logs at the specified level.
func NewLevelFactory(l log.Logger, lvl record.Level) amqp.MiddlewareFactory {
	return NewFactory(&simplifiedLogger{logger: l, level: lvl})
}

// NewDebugFactory creates a middleware factory that logs at debug level.
func NewDebugFactory(l log.Logger) amqp.MiddlewareFactory {
	return NewLevelFactory(l, record.Debug)
}

type factory struct {
	l Logger
}

func (f *factory) Wrap(b amqp.Broker) amqp.Broker {
	lb := broker{b: b, l: f.l}

	if ac, ok := b.(amqp.AnonymousConsumer); ok {
		return &extendedBroker{
			AnonymousConsumer: &anonymousConsumer{c: ac, l: f.l},
			Broker:            &lb,
		}
	}

	return &lb
}

type extendedBroker struct {
	amqp.AnonymousConsumer
	amqp.Broker
}

type anonymousConsumer struct {
	c amqp.AnonymousConsumer
	l Logger
}

func (ac *anonymousConsumer) ConsumeAnonymous(ctx context.Context, opts amqp.ConsumeOptions) (string, amqp.Consumer, error) {
	t0 := time.Now()
	queue, consumer, err := ac.c.ConsumeAnonymous(ctx, opts)

	ac.l.Log(
		"ConsumeAnonymous",
		err,
		time.Since(t0),
		log.Field("queue", queue),
		log.Field("consumer_tag", opts.Consumer),
		log.Field("auto_ack", opts.AutoACK),
	)

	if err != nil {
		return "", nil, err //nolint:wrapcheck
	}

	return queue, &consumerWrapper{Consumer: consumer, l: ac.l, queue: queue}, nil
}

type broker struct {
	b amqp.Broker
	l Logger
}

func (b *broker) Unwrap() amqp.Broker {
	return b.b
}

func (b *broker) Close() error {
	t0 := time.Now()
	err := b.b.Close()
	b.l.Log("Close", err, time.Since(t0))

	return err //nolint:wrapcheck
}

func (b *broker) Publish(ctx context.Context, exchange, key string, msg amqp.Message, opts amqp.PublishOptions) error {
	t0 := time.Now()
	err := b.b.Publish(ctx, exchange, key, msg, opts)
	b.l.Log(
		"Publish",
		err,
		time.Since(t0),
		log.Field("exchange", exchange),
		log.Field("routing_key", key),
		log.Field("message_id", msg.MessageID),
		log.Field("content_type", msg.ContentType),
		log.Field("body_size", len(msg.Body)),
	)

	return err //nolint:wrapcheck
}

func (b *broker) Consume(ctx context.Context, queue string, opts amqp.ConsumeOptions) (amqp.Consumer, error) {
	t0 := time.Now()
	c, err := b.b.Consume(ctx, queue, opts)
	b.l.Log(
		"Consume",
		err,
		time.Since(t0),
		log.Field("queue", queue),
		log.Field("consumer_tag", opts.Consumer),
		log.Field("auto_ack", opts.AutoACK),
	)

	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	return &consumerWrapper{Consumer: c, l: b.l, queue: queue}, nil
}

func (b *broker) Qos(ctx context.Context, opts amqp.QosOptions) error {
	t0 := time.Now()
	err := b.b.Qos(ctx, opts)
	b.l.Log(
		"Qos",
		err,
		time.Since(t0),
		log.Field("prefetch_count", opts.PrefetchCount),
		log.Field("prefetch_size", opts.PrefetchSize),
		log.Field("global", opts.Global),
	)

	return err //nolint:wrapcheck
}

func (b *broker) DeclareQueue(ctx context.Context, name string, opts amqp.DeclareQueueOptions) error {
	t0 := time.Now()
	err := b.b.DeclareQueue(ctx, name, opts)
	b.l.Log(
		"DeclareQueue",
		err,
		time.Since(t0),
		log.Field("queue", name),
		log.Field("durable", opts.Durable),
		log.Field("auto_delete", opts.AutoDelete),
	)

	return err //nolint:wrapcheck
}

func (b *broker) DeclareExchange(ctx context.Context, name string, kind amqp.ExchangeKind, opts amqp.DeclareExchangeOptions) error {
	t0 := time.Now()
	err := b.b.DeclareExchange(ctx, name, kind, opts)
	b.l.Log(
		"DeclareExchange",
		err,
		time.Since(t0),
		log.Field("exchange", name),
		log.Field("kind", kind),
		log.Field("durable", opts.Durable),
		log.Field("auto_delete", opts.AutoDelete),
	)

	return err //nolint:wrapcheck
}

func (b *broker) BindQueue(ctx context.Context, queue, key, exchange string, opts amqp.BindQueueOptions) error {
	t0 := time.Now()
	err := b.b.BindQueue(ctx, queue, key, exchange, opts)
	b.l.Log(
		"BindQueue",
		err,
		time.Since(t0),
		log.Field("queue", queue),
		log.Field("exchange", exchange),
		log.Field("routing_key", key),
	)

	return err //nolint:wrapcheck
}

// consumerWrapper wraps an amqp.Consumer to add logging around Close, Ack,
// Nack, and errors from Next. It embeds the inner consumer so that any
// additional interfaces it implements (e.g. consumer.Consumer with QueueName)
// are transparently promoted.
type consumerWrapper struct {
	amqp.Consumer

	l     Logger
	queue string
}

func (cw *consumerWrapper) Close() error {
	t0 := time.Now()
	err := cw.Consumer.Close()
	cw.l.Log(
		"Consumer.Close",
		err,
		time.Since(t0),
		log.Field("queue", cw.queue),
	)

	return err //nolint:wrapcheck
}

func (cw *consumerWrapper) Next(ctx context.Context) (*amqp.Delivery, error) {
	delivery, err := cw.Consumer.Next(ctx)

	// Only log when something went wrong; successful deliveries are high-volume
	// and the blocking wait time is not meaningful as a latency signal.
	if err != nil {
		cw.l.Log("Consumer.Next", err, 0, log.Field("queue", cw.queue))
	}

	return delivery, err //nolint:wrapcheck
}

func (cw *consumerWrapper) Ack(ctx context.Context, tag uint64, opts amqp.AckOptions) error {
	t0 := time.Now()
	err := cw.Consumer.Ack(ctx, tag, opts)
	cw.l.Log(
		"Consumer.Ack",
		err,
		time.Since(t0),
		log.Field("queue", cw.queue),
		log.Field("delivery_tag", tag),
		log.Field("multiple", opts.Multiple),
	)

	return err //nolint:wrapcheck
}

func (cw *consumerWrapper) Nack(ctx context.Context, tag uint64, opts amqp.NackOptions) error {
	t0 := time.Now()
	err := cw.Consumer.Nack(ctx, tag, opts)
	cw.l.Log(
		"Consumer.Nack",
		err,
		time.Since(t0),
		log.Field("queue", cw.queue),
		log.Field("delivery_tag", tag),
		log.Field("multiple", opts.Multiple),
		log.Field("requeue", opts.Requeue),
	)

	return err //nolint:wrapcheck
}

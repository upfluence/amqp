// Package amqputil provides utility functions for creating AMQP brokers with common configurations.
package amqputil

import (
	"github.com/upfluence/pkg/v2/envutil"
	"github.com/upfluence/pkg/v2/peer"

	"github.com/upfluence/amqp"
	"github.com/upfluence/amqp/backend/rabbitmq"
)

// peerTable converts peer information to AMQP connection properties.
// These properties are visible in the RabbitMQ management UI.
func peerTable(p *peer.Peer) map[string]interface{} {
	if p == nil {
		return nil
	}

	return map[string]interface{}{
		"upfluence-unit-name":    p.InstanceName,
		"upfluence-app-name":     p.AppName,
		"upfluence-project-name": p.ProjectName,
		"upfluence-env":          p.Environment,
		"upfluence-version":      p.Version.String(),
	}
}

// Option configures the broker builder.
type Option func(*builder)

// WithURL sets the AMQP connection URL.
func WithURL(uri string) Option {
	return func(b *builder) { b.uri = uri }
}

// WithMiddleware adds a middleware factory to wrap the broker.
func WithMiddleware(f amqp.MiddlewareFactory) Option {
	return func(b *builder) { b.middlewares = append(b.middlewares, f) }
}

// WithBrokerOption passes a rabbitmq.Option directly to the underlying broker.
// Use this to configure low-level rabbitmq behaviour (e.g. channel pool size,
// reconnect delay) that is not exposed through the higher-level Option API.
func WithBrokerOption(opt rabbitmq.Option) Option {
	return func(b *builder) { b.backendOpts = append(b.backendOpts, opt) }
}

// builder accumulates configuration for creating a broker.
type builder struct {
	uri string

	peer        *peer.Peer
	middlewares []amqp.MiddlewareFactory
	backendOpts []rabbitmq.Option
}

// options converts builder configuration to rabbitmq options.
func (b *builder) options() []rabbitmq.Option {
	return append(
		[]rabbitmq.Option{rabbitmq.WithProperties(peerTable(b.peer))},
		b.backendOpts...,
	)
}

// Open creates a new AMQP broker with environment-based configuration.
func Open(opts ...Option) amqp.Broker {
	b := builder{
		uri:  envutil.FetchString("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2f"),
		peer: peer.FromEnv(),
	}

	for _, opt := range opts {
		opt(&b)
	}

	var res amqp.Broker = rabbitmq.NewBroker(b.uri, b.options()...)

	for _, m := range b.middlewares {
		res = m.Wrap(res)
	}

	return res
}

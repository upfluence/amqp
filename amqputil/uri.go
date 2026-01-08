package amqputil

import (
	"github.com/upfluence/pkg/v2/envutil"
	"github.com/upfluence/pkg/v2/peer"

	"github.com/upfluence/amqp"
	"github.com/upfluence/amqp/backend"
)

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

type Option func(*builder)

func WithURL(uri string) Option {
	return func(b *builder) { b.uri = uri }
}

func WithMiddleware(f amqp.MiddlewareFactory) Option {
	return func(b *builder) { b.middlewares = append(b.middlewares, f) }
}

type builder struct {
	uri string

	peer        *peer.Peer
	middlewares []amqp.MiddlewareFactory
}

func (b *builder) options() []backend.Option {
	return []backend.Option{
		backend.WithProperties(peerTable(b.peer)),
	}
}

func Open(opts ...Option) amqp.Broker {
	b := builder{
		uri:  envutil.FetchString("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2f"),
		peer: peer.FromEnv(),
	}

	for _, opt := range opts {
		opt(&b)
	}

	var res amqp.Broker = backend.NewBroker(b.uri, b.options()...)

	for _, m := range b.middlewares {
		res = m.Wrap(res)
	}

	return res
}

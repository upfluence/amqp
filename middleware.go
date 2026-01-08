package amqp

type MiddlewareFactory interface {
	Wrap(Broker) Broker
}

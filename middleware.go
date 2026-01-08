package amqp

// MiddlewareFactory creates middleware that wraps a Broker to add cross-cutting concerns.
//
// Middleware can be used to add functionality like logging, metrics, tracing, or rate limiting
// to broker operations without modifying the broker implementation.
type MiddlewareFactory interface {
	// Wrap takes a Broker and returns a new Broker that wraps it with additional functionality.
	// The wrapped broker should delegate all operations to the underlying broker while
	// adding its own behavior before or after the delegated calls.
	Wrap(Broker) Broker
}

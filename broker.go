// Package amqp provides a high-level interface for working with AMQP 0.9.1 (RabbitMQ).
//
// This package offers connection pooling, automatic reconnection, middleware support,
// and a clean API for publishing and consuming messages. It abstracts away the complexity
// of managing connections and channels while providing production-ready features like
// rate limiting and error handling.
package amqp

import (
	"context"
	"io"
	"time"
)

// PublishOptions contains options for publishing messages.
type PublishOptions struct {
	// Mandatory causes the server to return an unroutable message with a Return method.
	// If this flag is false, the server silently drops the message.
	Mandatory bool

	// Immediate causes the server to return a message if it cannot be routed to a queue
	// consumer immediately. If this flag is false, the server queues the message.
	// Note: This flag is not supported by RabbitMQ and may be deprecated.
	Immediate bool
}

// Message represents an AMQP message with all standard properties.
type Message struct {
	// Body is the message payload.
	Body []byte

	// Headers contains custom application headers.
	Headers map[string]any

	// ContentType is the MIME content type (e.g., "application/json", "text/plain").
	ContentType string

	// ContentEncoding is the MIME content encoding (e.g., "utf-8", "gzip").
	ContentEncoding string

	// DeliveryMode controls message persistence.
	// 0 or 1 = transient (in-memory only), 2 = persistent (written to disk).
	DeliveryMode uint8

	// Priority is the message priority from 0 (lowest) to 9 (highest).
	Priority uint8

	// CorrelationID is used to correlate RPC requests with responses.
	CorrelationID string

	// ReplyTo is the address to send replies to (e.g., for RPC patterns).
	ReplyTo string

	// Expiration is the message expiration time in milliseconds as a string.
	Expiration string

	// MessageID is an application-specific message identifier.
	MessageID string

	// Timestamp is the message creation timestamp.
	Timestamp time.Time

	// Type is an application-specific message type name.
	Type string

	// UserID is the creating user ID (must match the connection user).
	UserID string

	// AppID is the creating application identifier.
	AppID string
}

// ConsumeOptions contains options for starting a consumer.
type ConsumeOptions struct {
	// Consumer is the consumer tag, which can be used to cancel the consumer.
	// If empty, the server generates a unique tag.
	Consumer string

	// AutoACK when true, the server automatically acknowledges deliveries.
	// When false, the client must manually acknowledge using Ack/Nack.
	AutoACK bool

	// Exclusive when true, the consumer is the only consumer on this queue.
	// The queue will be deleted when the consumer disconnects.
	Exclusive bool

	// Args contains optional arguments for the consumer.
	Args map[string]any
}

// DeclareQueueOptions contains options for declaring a queue.
type DeclareQueueOptions struct {
	// Passive when true, only checks if the queue exists without creating it.
	// Returns an error if the queue doesn't exist.
	Passive bool

	// Durable when true, the queue survives broker restarts.
	// Messages in durable queues are only persisted if they have DeliveryMode=2.
	Durable bool

	// AutoDelete when true, the queue is deleted when the last consumer disconnects.
	AutoDelete bool

	// Exclusive when true, the queue is scoped to this connection and deleted when
	// the connection closes.
	Exclusive bool

	// Args contains optional queue arguments.
	Args map[string]any
}

// DeclareExchangeOptions contains options for declaring an exchange.
type DeclareExchangeOptions struct {
	// Passive when true, only checks if the exchange exists without creating it.
	// Returns an error if the exchange doesn't exist.
	Passive bool

	// Durable when true, the exchange survives broker restarts.
	Durable bool

	// AutoDelete when true, the exchange is deleted when all queues are unbound.
	AutoDelete bool

	// Args contains optional exchange arguments.
	Args map[string]any
}

// ExchangeKind represents the type of AMQP exchange.
type ExchangeKind string

const (
	// Direct exchanges route messages to queues based on exact routing key matches.
	Direct ExchangeKind = "direct"

	// Fanout exchanges route messages to all bound queues, ignoring the routing key.
	Fanout ExchangeKind = "fanout"
)

// BindQueueOptions contains options for binding a queue to an exchange.
type BindQueueOptions struct {
	// Args contains optional binding arguments.
	Args map[string]any
}

// Delivery represents a received message with its delivery metadata.
type Delivery struct {
	// Message is the received message with all properties.
	Message Message

	// DeliveryTag is a server-assigned delivery identifier used for acknowledgement.
	DeliveryTag uint64
	// Redelivered bool
	// Exchange    string
	// RoutingKey  string
}

// QosOptions contains Quality of Service options for message delivery.
type QosOptions struct {
	// PrefetchCount is the maximum number of unacknowledged messages.
	// 0 means unlimited.
	PrefetchCount int

	// PrefetchSize is the maximum size in bytes of unacknowledged messages.
	// 0 means unlimited.
	PrefetchSize int

	// Global when true, applies QoS settings to the entire connection.
	// When false, applies only to the current channel.
	Global bool
}

// AckOptions contains options for acknowledging messages.
type AckOptions struct {
	// Multiple when true, acknowledges all messages up to and including
	// the delivery tag. When false, acknowledges only the specified message.
	Multiple bool
}

// NackOptions contains options for negatively acknowledging (rejecting) messages.
type NackOptions struct {
	AckOptions

	// Requeue when true, the message is requeued for redelivery.
	// When false, the message is discarded (or sent to dead letter exchange if configured).
	Requeue bool
}

// Consumer represents an active consumer on a queue.
type Consumer interface {
	io.Closer

	// IsOpen returns true if the consumer is still active and can receive messages.
	IsOpen() bool

	// Next waits for and returns the next message delivery.
	// Blocks until a message is available or the context is cancelled.
	// Returns an error if the consumer is closed or the context is cancelled.
	Next(context.Context) (*Delivery, error)

	// Ack positively acknowledges a message delivery.
	// The broker will not redeliver acknowledged messages.
	Ack(context.Context, uint64, AckOptions) error

	// Nack negatively acknowledges a message delivery.
	// Depending on options, the message may be requeued or discarded.
	Nack(context.Context, uint64, NackOptions) error
}

// Broker is the main interface for interacting with AMQP.
type Broker interface {
	io.Closer

	// Publish sends a message to an exchange with a routing key.
	// Use an empty exchange ("") to send to the default exchange,
	// in which case the routing key should be the queue name.
	Publish(context.Context, string, string, Message, PublishOptions) error

	// Consume starts consuming messages from a queue.
	// Returns a Consumer that can be used to receive messages.
	// The Consumer must be closed when no longer needed.
	Consume(context.Context, string, ConsumeOptions) (Consumer, error)

	// Qos sets Quality of Service parameters for message delivery.
	// Controls prefetch count and size to limit unacknowledged messages.
	Qos(context.Context, QosOptions) error

	// DeclareQueue creates a queue or verifies that it exists with the given parameters.
	// If the queue already exists with the same parameters, this is idempotent.
	DeclareQueue(context.Context, string, DeclareQueueOptions) error

	// DeclareExchange creates an exchange or verifies that it exists.
	// If the exchange already exists with the same parameters, this is idempotent.
	DeclareExchange(context.Context, string, ExchangeKind, DeclareExchangeOptions) error

	// BindQueue creates a binding between a queue and an exchange with a routing key.
	// For fanout exchanges, the routing key is ignored but still required.
	BindQueue(context.Context, string, string, string, BindQueueOptions) error
}

// AnonymousConsumer is an optional interface that brokers may implement
// to support creating consumers with automatically generated queue names.
type AnonymousConsumer interface {
	// ConsumeAnonymous creates a temporary queue with an auto-generated name
	// and starts consuming from it. Returns the queue name and the consumer.
	// The queue is typically configured with AutoDelete=true.
	ConsumeAnonymous(context.Context, ConsumeOptions) (string, Consumer, error)
}

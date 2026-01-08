package amqp

import (
	"context"
	"io"
	"time"
)

type PublishOptions struct {
	Mandatory bool
	Immediate bool
}

type Message struct {
	Body []byte

	Headers map[string]any

	ContentType     string    // MIME content type
	ContentEncoding string    // MIME content encoding
	DeliveryMode    uint8     // Transient (0 or 1) or Persistent (2)
	Priority        uint8     // 0 to 9
	CorrelationID   string    // correlation identifier
	ReplyTo         string    // address to to reply to (ex: RPC)
	Expiration      string    // message expiration spec
	MessageID       string    // message identifier
	Timestamp       time.Time // message timestamp
	Type            string    // message type name
	UserID          string    // creating user id - ex: "guest"
	AppID           string    // creating application id
}

type ConsumeOptions struct {
	Consumer string

	AutoACK   bool
	Exclusive bool

	Args map[string]any
}

type DeclareQueueOptions struct {
	Passive bool

	Durable    bool
	AutoDelete bool

	Args map[string]any
}

type DeclareExchangeOptions struct {
	Passive bool

	Durable    bool
	AutoDelete bool

	Args map[string]any
}

type ExchangeKind string

const (
	Direct ExchangeKind = "direct"
	Fanout ExchangeKind = "fanout"
)

type BindQueueOptions struct {
	Args map[string]any
}

type Delivery struct {
	Message Message

	DeliveryTag uint64
	// Redelivered bool
	// Exchange    string
	// RoutingKey  string
}

type QosOptions struct {
	PrefetchCount int
	PrefetchSize  int
	Global        bool
}

type AckOptions struct {
	Multiple bool
}

type NackOptions struct {
	AckOptions

	Requeue bool
}

type Consumer interface {
	io.Closer

	IsOpen() bool

	Next(context.Context) (*Delivery, error)

	Ack(context.Context, uint64, AckOptions) error
	Nack(context.Context, uint64, NackOptions) error
}

type Broker interface {
	io.Closer

	Publish(context.Context, string, string, Message, PublishOptions) error

	Consume(context.Context, string, ConsumeOptions) (Consumer, error)

	Qos(context.Context, QosOptions) error

	DeclareQueue(context.Context, string, DeclareQueueOptions) error
	DeclareExchange(context.Context, string, ExchangeKind, DeclareExchangeOptions) error

	BindQueue(context.Context, string, string, string, BindQueueOptions) error
}

type AnonymousConsumer interface {
	ConsumeAnonymous(context.Context, ConsumeOptions) (string, Consumer, error)
}

# AMQP

A robust Go library for working with AMQP (RabbitMQ) that provides connection pooling, automatic reconnection, middleware support, and a clean interface for publishing and consuming messages.

## Features

- **Connection and Channel Pooling**: Efficient resource management with configurable pool sizes
- **Automatic Reconnection**: Built-in connection recovery with rate limiting
- **Middleware Support**: Extensible middleware system for logging, metrics, and more
- **Clean API**: Simple, context-aware interfaces for publishing and consuming
- **Consumer Pools**: Manage multiple consumers with automatic lifecycle management
- **Anonymous Queues**: Easy creation of temporary, auto-delete queues
- **Statistics**: Real-time monitoring of connection and channel states

## Installation

```bash
go get github.com/upfluence/amqp
```

## Quick Start

### Basic Publishing

```go
package main

import (
    "context"
    "github.com/upfluence/amqp"
    "github.com/upfluence/amqp/amqputil"
)

func main() {
    // Create a broker
    broker := amqputil.Open()
    defer broker.Close()

    // Publish a message
    ctx := context.Background()
    err := broker.Publish(
        ctx,
        "my-exchange",
        "routing.key",
        amqp.Message{
            Body:        []byte("Hello, World!"),
            ContentType: "text/plain",
        },
        amqp.PublishOptions{},
    )
    if err != nil {
        panic(err)
    }
}
```

### Basic Consuming

```go
// Declare a queue
err := broker.DeclareQueue(ctx, "my-queue", amqp.DeclareQueueOptions{
    Durable: true,
})
if err != nil {
    panic(err)
}

// Start consuming
consumer, err := broker.Consume(ctx, "my-queue", amqp.ConsumeOptions{
    AutoACK: false,
})
if err != nil {
    panic(err)
}
defer consumer.Close()

// Process messages
for {
    delivery, err := consumer.Next(ctx)
    if err != nil {
        // Handle error
        break
    }

    // Process the message
    println(string(delivery.Message.Body))

    // Acknowledge the message
    err = consumer.Ack(ctx, delivery.DeliveryTag, amqp.AckOptions{})
    if err != nil {
        // Handle error
    }
}
```

## Advanced Usage

### Using Consumer Pools

Consumer pools help manage multiple consumers efficiently:

```go
import "github.com/upfluence/amqp/consumer"

// Create a consumer pool
pool := consumer.NewConsumerPool(
    broker,
    amqp.ConsumeOptions{AutoACK: false},
)
defer pool.Close()

// Get a consumer from the pool
cons, err := pool.Get(ctx)
if err != nil {
    panic(err)
}

// Use the consumer
delivery, err := cons.Next(ctx)
// ... process message ...

// Return to pool when done
pool.Put(ctx, cons)
```

### Anonymous Queues

Anonymous queues are automatically created and deleted:

```go
import "github.com/upfluence/amqp/consumer"

// Create an anonymous consumer
cons, err := consumer.BuildConsumer(ctx, broker, amqp.ConsumeOptions{})
if err != nil {
    panic(err)
}

// Get the auto-generated queue name
queueName := cons.QueueName()

// Publish to this queue
broker.Publish(ctx, "", queueName, message, amqp.PublishOptions{})
```

### Middleware

Add logging and observability to your broker:

```go
import (
    "github.com/upfluence/amqp/amqputil"
    "github.com/upfluence/amqp/middleware/logger"
    "github.com/upfluence/log"
)

// Create broker with logging middleware
broker := amqputil.Open(
    amqputil.WithMiddleware(logger.NewDebugFactory(log.DefaultLogger)),
)

// Now all operations will be logged
broker.Publish(ctx, "exchange", "key", message, amqp.PublishOptions{})
```

### Queue and Exchange Declaration

```go
// Declare a durable queue
err := broker.DeclareQueue(ctx, "tasks", amqp.DeclareQueueOptions{
    Durable:    true,
    AutoDelete: false,
})

// Declare an exchange
err = broker.DeclareExchange(
    ctx,
    "logs",
    amqp.Fanout,
    amqp.DeclareExchangeOptions{
        Durable: true,
    },
)

// Bind queue to exchange
err = broker.BindQueue(ctx, "tasks", "task.*", "work", amqp.BindQueueOptions{})
```

### Quality of Service (QoS)

Control message prefetch:

```go
err := broker.Qos(ctx, amqp.QosOptions{
    PrefetchCount: 10,  // Prefetch 10 messages at a time
    PrefetchSize:  0,   // No limit on message size
    Global:        false,
})
```

### Message Options

```go
message := amqp.Message{
    Body:            []byte("message body"),
    ContentType:     "application/json",
    ContentEncoding: "utf-8",
    DeliveryMode:    2,  // Persistent
    Priority:        5,
    CorrelationID:   "correlation-123",
    ReplyTo:         "reply-queue",
    MessageID:       "msg-456",
    Timestamp:       time.Now(),
    Headers: map[string]any{
        "custom-header": "value",
    },
}
```

### Monitoring

Get real-time statistics:

```go
stats := broker.Stats()
fmt.Printf("Connection Open: %v\n", stats.ConnectionOpened)
fmt.Printf("Idle Channels: %d\n", stats.IdleChannel)
fmt.Printf("In-Use Channels: %d\n", stats.InUseChannel)
fmt.Printf("Consuming Channels: %d\n", stats.ConsumingChannel)
```

## Configuration Options

### Broker Options

```go
import "github.com/upfluence/amqp/amqputil"

// Using environment variable RABBITMQ_URL or default
broker := amqputil.Open()

// With custom URL
broker := amqputil.Open(
    amqputil.WithURL("amqp://localhost:5672/"),
)

// With middleware
broker := amqputil.Open(
    amqputil.WithMiddleware(logger.NewDebugFactory(log.DefaultLogger)),
)
```

### Consume Options

```go
options := amqp.ConsumeOptions{
    Consumer:  "consumer-tag",
    AutoACK:   false,
    Exclusive: false,
    Args: map[string]any{
        "x-priority": 10,
    },
}
```

## Error Handling

The library uses the `github.com/upfluence/errors` package for error wrapping and provides detailed error messages:

```go
err := broker.Publish(ctx, exchange, key, msg, opts)
if err != nil {
    // Error will include context about what failed
    log.Printf("Publish failed: %v", err)
}
```

## Best Practices

1. **Reuse Brokers**: Create one broker instance and reuse it across your application
2. **Close Resources**: Always defer Close() on brokers and consumers
3. **Use Context**: Pass context for proper cancellation and timeouts
4. **Handle Reconnections**: The library handles reconnections automatically, but handle consumer errors gracefully
5. **Use Middleware**: Add logging middleware for debugging and observability
6. **Pool Consumers**: Use consumer pools for high-throughput scenarios
7. **Set QoS**: Configure prefetch limits to prevent overwhelming consumers

## Architecture

The library follows a layered architecture:

- **broker.go**: Core interfaces (Broker, Consumer, Message types)
- **backend/**: RabbitMQ implementation with connection/channel pooling
- **consumer/**: Consumer pool management and anonymous queue handling
- **middleware/**: Extensible middleware system
- **amqputil/**: Utility functions for connection management

## Contributing

Contributions are welcome! Please ensure:

1. Tests pass: `go test ./...`
2. Code is formatted: `go fmt ./...`
3. Linting passes: See `.github/workflows/lint.yml`

## License

See LICENSE file for details.

## See Also

- [RabbitMQ Documentation](https://www.rabbitmq.com/documentation.html)
- [AMQP 0-9-1 Protocol](https://www.rabbitmq.com/amqp-0-9-1-reference.html)

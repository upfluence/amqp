package rabbitmq_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/upfluence/amqp"
	"github.com/upfluence/amqp/amqptest"
	"github.com/upfluence/amqp/backend/rabbitmq"
)

func TestBroker(t *testing.T) {
	amqptest.NewTestCase().Run(t, func(t *testing.T, broker amqp.Broker) {
		ctx := context.Background()

		exchangeName := fmt.Sprintf("test-exchange-%d", time.Now().UnixNano())
		queueName := fmt.Sprintf("test-queue-%d", time.Now().UnixNano())
		routingKey := "test.routing.key"

		err := broker.DeclareExchange(ctx, exchangeName, amqp.Direct, amqp.DeclareExchangeOptions{
			Durable:    false,
			AutoDelete: true,
		})
		require.NoError(t, err, "Failed to declare exchange")

		err = broker.DeclareQueue(ctx, queueName, amqp.DeclareQueueOptions{
			Durable:    false,
			AutoDelete: true,
			Exclusive:  true,
		})
		require.NoError(t, err, "Failed to declare queue")

		err = broker.BindQueue(ctx, queueName, routingKey, exchangeName, amqp.BindQueueOptions{})
		require.NoError(t, err, "Failed to bind queue")

		messageBody := []byte("Hello, AMQP!")
		messageID := fmt.Sprintf("msg-%d", time.Now().UnixNano())
		msg := amqp.Message{
			Body:        messageBody,
			ContentType: "text/plain",
			MessageID:   messageID,
			Timestamp:   time.Now(),
		}

		err = broker.Publish(ctx, exchangeName, routingKey, msg, amqp.PublishOptions{})
		require.NoError(t, err, "Failed to publish message")

		consumer, err := broker.Consume(ctx, queueName, amqp.ConsumeOptions{
			AutoACK: false,
		})
		require.NoError(t, err, "Failed to start consumer")

		defer consumer.Close()

		delivery, err := consumer.Next(ctx)
		require.NoError(t, err, "Failed to receive message")

		assert.Equal(t, messageBody, delivery.Message.Body, "Body mismatch")
		assert.Equal(t, messageID, delivery.Message.MessageID, "MessageID mismatch")
		assert.Equal(t, "text/plain", delivery.Message.ContentType, "ContentType mismatch")

		err = consumer.Ack(ctx, delivery.DeliveryTag, amqp.AckOptions{})
		require.NoError(t, err, "Failed to acknowledge message")
	})
}

func TestBrokerConsumeAnonymously(t *testing.T) {
	amqptest.NewTestCase().Run(t, func(t *testing.T, broker amqp.Broker) {
		ctx := context.Background()

		b, ok := broker.(amqp.AnonymousConsumer)
		require.True(t, ok, "Broker does not implement ConsumeAnonymous")

		queueName, consumer, err := b.ConsumeAnonymous(ctx, amqp.ConsumeOptions{
			AutoACK: false,
		})
		require.NoError(t, err, "Failed to start anonymous consumer")
		require.NotEmpty(t, queueName, "Queue name should not be empty")

		defer consumer.Close()

		messageBody := []byte("Hello, Anonymous Queue!")
		messageID := fmt.Sprintf("msg-%d", time.Now().UnixNano())
		msg := amqp.Message{
			Body:        messageBody,
			ContentType: "application/json",
			MessageID:   messageID,
			Timestamp:   time.Now(),
		}

		err = broker.Publish(ctx, "", queueName, msg, amqp.PublishOptions{})
		require.NoError(t, err, "Failed to publish message to anonymous queue")

		delivery, err := consumer.Next(ctx)
		require.NoError(t, err, "Failed to receive message")

		assert.Equal(t, messageBody, delivery.Message.Body, "Body mismatch")
		assert.Equal(t, messageID, delivery.Message.MessageID, "MessageID mismatch")
		assert.Equal(t, "application/json", delivery.Message.ContentType, "ContentType mismatch")

		err = consumer.Ack(ctx, delivery.DeliveryTag, amqp.AckOptions{})
		require.NoError(t, err, "Failed to acknowledge message")
	})
}

func TestBrokerStatsConsumingChannel(t *testing.T) {
	amqptest.NewTestCase().Run(t, func(t *testing.T, broker amqp.Broker) {
		ctx := context.Background()

		queueName := fmt.Sprintf("test-stats-queue-%d", time.Now().UnixNano())

		err := broker.DeclareQueue(ctx, queueName, amqp.DeclareQueueOptions{
			Durable:    false,
			AutoDelete: true,
			Exclusive:  true,
		})
		require.NoError(t, err, "Failed to declare queue")

		consumer, err := broker.Consume(ctx, queueName, amqp.ConsumeOptions{})
		require.NoError(t, err, "Failed to start consumer")

		stats := rabbitmq.GetStats(broker)
		assert.Equal(t, 1, stats.ConsumingChannel)

		err = consumer.Close()
		require.NoError(t, err, "Failed to close consumer")

		stats = rabbitmq.GetStats(broker)
		assert.Equal(t, 0, stats.ConsumingChannel)
	})
}

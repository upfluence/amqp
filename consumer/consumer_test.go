package consumer_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/upfluence/amqp"
	"github.com/upfluence/amqp/amqptest"
	"github.com/upfluence/amqp/consumer"
)

type brokerWrapper struct {
	amqp.Broker
}

func TestBuildConsumer(t *testing.T) {
	amqptest.NewTestCase().Run(t, func(t *testing.T, broker amqp.Broker) {
		for name, fn := range map[string]func(amqp.Broker) amqp.Broker{
			"with ConsumeAnonymous": func(b amqp.Broker) amqp.Broker { return b },
			"without ConsumeAnonymous": func(b amqp.Broker) amqp.Broker {
				return brokerWrapper{Broker: b}
			},
		} {
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()

				c, err := consumer.BuildConsumer(ctx, fn(broker), amqp.ConsumeOptions{})
				require.NoError(t, err, "Failed to build consumer")
				require.NotNil(t, c, "Consumer should not be nil")

				defer c.Close()

				queueName := c.QueueName()
				require.NotEmpty(t, queueName, "Queue name should not be empty")

				assert.True(t, c.IsOpen(), "Consumer should be open")

				messageBody := []byte("Test message for BuildConsumer")
				messageID := fmt.Sprintf("msg-%d", time.Now().UnixNano())
				msg := amqp.Message{
					Body:        messageBody,
					ContentType: "text/plain",
					MessageID:   messageID,
				}

				err = broker.Publish(ctx, "", c.QueueName(), msg, amqp.PublishOptions{})
				require.NoError(t, err, "Failed to publish message")

				delivery, err := c.Next(ctx)
				require.NoError(t, err, "Failed to receive message")

				assert.Equal(t, messageBody, delivery.Message.Body, "Body mismatch")
				assert.Equal(t, messageID, delivery.Message.MessageID, "MessageID mismatch")
				assert.Equal(t, "text/plain", delivery.Message.ContentType, "ContentType mismatch")

				err = c.Ack(ctx, delivery.DeliveryTag, amqp.AckOptions{})
				require.NoError(t, err, "Failed to acknowledge message")
			})
		}
	})
}

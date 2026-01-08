package amqptest

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/upfluence/amqp"
	"github.com/upfluence/amqp/amqputil"
	"github.com/upfluence/amqp/middleware/logger"
	"github.com/upfluence/log/record"
)

type testLogger struct {
	testing.TB
}

func (tl testLogger) Log(operation string, err error, d time.Duration, fs ...record.Field) {
	var b = fmt.Sprintf("[duration: %v]", d)

	for _, f := range fs {
		b += fmt.Sprintf("[%s: %v]", f.GetKey(), f.GetValue())
	}

	if err != nil {
		b += fmt.Sprintf("[error: %v]", err)
	}

	b += " " + operation

	tl.TB.Log(b)
}

type TestCase struct {
	amqpURL string

	opts []amqputil.Option
}

type TestCaseOption func(*TestCase)

// NewTestCase creates a new test case that can run tests against RabbitMQ.
// By default, it uses the RABBITMQ_URL environment variable, or skips the test if not set.
func NewTestCase(opts ...TestCaseOption) *TestCase {
	var tc = TestCase{
		amqpURL: os.Getenv("RABBITMQ_URL"),
	}

	for _, opt := range opts {
		opt(&tc)
	}

	return &tc
}

func (tc *TestCase) buildBroker(t *testing.T, url string) amqp.Broker {
	return amqputil.Open(
		append(
			tc.opts,
			amqputil.WithURL(url),
			amqputil.WithMiddleware(
				logger.NewFactory(testLogger{t}),
			),
		)...,
	)
}

// Run executes the test function with a configured AMQP broker.
// It will run the test against RabbitMQ if RABBITMQ_URL is set,
// otherwise it will skip the test.
func (tc *TestCase) Run(t *testing.T, fn func(t *testing.T, broker amqp.Broker)) {
	t.Helper()

	if tc.amqpURL == "" {
		t.Skip("No RABBITMQ_URL environment variable set, skipping test case")
		return
	}

	t.Run("rabbitmq", func(t *testing.T) {
		broker := tc.buildBroker(t, tc.amqpURL)
		defer broker.Close()

		// Clean up any existing queues/exchanges by creating a temporary queue
		// and ensuring the connection works
		ctx := context.Background()
		testQueue := fmt.Sprintf("test-cleanup-%d", time.Now().UnixNano())

		// Declare and delete a test queue to ensure connection is working
		err := broker.DeclareQueue(ctx, testQueue, amqp.DeclareQueueOptions{
			AutoDelete: true,
		})

		if err != nil {
			t.Fatalf("Failed to connect to RabbitMQ: %v", err)
		}

		fn(t, broker)
	})
}

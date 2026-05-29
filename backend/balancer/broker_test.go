package balancer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/upfluence/amqp"
)

type testConsumer struct {
	amqp.Consumer

	nextErr  error
	closeErr error
}

func (c *testConsumer) Next(context.Context) (*amqp.Delivery, error) {
	return nil, c.nextErr
}

func (c *testConsumer) Close() error {
	return c.closeErr
}

func TestConsumerIgnoresNextContextErrors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		haveErr error
	}{
		{name: "canceled", haveErr: context.Canceled},
		{name: "deadline exceeded", haveErr: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := 0
			c := newConsumer(&testConsumer{nextErr: tc.haveErr}, func(error) { called++ })

			_, err := c.Next(context.Background())
			require.ErrorIs(t, err, tc.haveErr)
			assert.Equal(t, 0, called)
		})
	}
}

func TestConsumerNotifiesOnce(t *testing.T) {
	called := 0
	c := newConsumer(&testConsumer{nextErr: assert.AnError}, func(error) { called++ })

	_, err := c.Next(context.Background())
	require.ErrorIs(t, err, assert.AnError)

	err = c.Close()
	require.NoError(t, err)

	assert.Equal(t, 1, called)
}

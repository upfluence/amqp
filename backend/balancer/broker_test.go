package balancer_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/upfluence/errors"
	pbalancer "github.com/upfluence/pkg/v2/discovery/balancer"

	"github.com/upfluence/amqp"
	"github.com/upfluence/amqp/backend/balancer"
	"github.com/upfluence/amqp/backend/rabbitmq"
)

const (
	declareQueue = "DeclareQueue"
	bindQueue    = "BindQueue"
	consumeQueue = "Consume"
)

var errFake = errors.New("fake error")

type call struct {
	broker int
	method string
	queue  string
}

type recorder struct {
	calls []call
}

type fakeBroker struct {
	amqp.Broker

	id           int
	rec          *recorder
	err          error
	disconnected bool
}

func (b *fakeBroker) record(method, queue string) error {
	b.rec.calls = append(b.rec.calls, call{broker: b.id, method: method, queue: queue})

	return b.err
}

func (b *fakeBroker) Stats() rabbitmq.BrokerStats {
	return rabbitmq.BrokerStats{ConnectionOpened: !b.disconnected}
}

func (b *fakeBroker) DeclareQueue(_ context.Context, queue string, _ amqp.DeclareQueueOptions) error {
	return b.record(declareQueue, queue)
}

func (b *fakeBroker) BindQueue(_ context.Context, queue, _, _ string, _ amqp.BindQueueOptions) error {
	return b.record(bindQueue, queue)
}

func (b *fakeBroker) Consume(_ context.Context, queue string, _ amqp.ConsumeOptions) (amqp.Consumer, error) {
	return nil, b.record(consumeQueue, queue)
}

// roundRobinBalancer hands out its peers in order, so that consecutive
// operations never land on the same broker unless they are pinned.
type roundRobinBalancer struct {
	peers []*fakeBroker
	next  int
}

func (b *roundRobinBalancer) Open(context.Context) error { return nil }
func (b *roundRobinBalancer) IsOpen() bool               { return true }
func (b *roundRobinBalancer) Close() error               { return nil }

func (b *roundRobinBalancer) Get(context.Context, pbalancer.GetOptions) (balancer.BrokerWrapper, func(error), error) {
	p := b.peers[b.next%len(b.peers)]

	b.next++

	return balancer.BrokerWrapper{Broker: p}, func(error) {}, nil
}

type op func(context.Context, *balancer.Broker, []*fakeBroker) error

func declare(queue string, exclusive bool) op {
	return func(ctx context.Context, b *balancer.Broker, _ []*fakeBroker) error {
		return b.DeclareQueue(ctx, queue, amqp.DeclareQueueOptions{Exclusive: exclusive})
	}
}

func bind(queue string) op {
	return func(ctx context.Context, b *balancer.Broker, _ []*fakeBroker) error {
		return b.BindQueue(ctx, queue, "key", "exchange", amqp.BindQueueOptions{})
	}
}

func consume(queue string) op {
	return func(ctx context.Context, b *balancer.Broker, _ []*fakeBroker) error {
		_, err := b.Consume(ctx, queue, amqp.ConsumeOptions{})

		return err
	}
}

func failWith(i int, err error) op {
	return func(_ context.Context, _ *balancer.Broker, bs []*fakeBroker) error {
		bs[i].err = err

		return nil
	}
}

func disconnect(i int) op {
	return func(_ context.Context, _ *balancer.Broker, bs []*fakeBroker) error {
		bs[i].disconnected = true

		return nil
	}
}

func TestBrokerExclusiveQueuePinning(t *testing.T) {
	for _, tt := range []struct {
		name      string
		haveErrs  []error
		haveOps   []op
		wantCalls []call
		wantErrs  []error
	}{
		{
			name:     "exclusive queue sticks to the declaring broker",
			haveErrs: []error{nil, nil},
			haveOps: []op{
				declare("q", true),
				bind("q"),
				consume("q"),
				declare("q", true),
			},
			wantCalls: []call{
				{broker: 0, method: declareQueue, queue: "q"},
				{broker: 0, method: bindQueue, queue: "q"},
				{broker: 0, method: consumeQueue, queue: "q"},
				{broker: 0, method: declareQueue, queue: "q"},
			},
			wantErrs: []error{nil, nil, nil, nil},
		},
		{
			name:     "non exclusive queue is balanced",
			haveErrs: []error{nil, nil},
			haveOps: []op{
				declare("q", false),
				bind("q"),
				consume("q"),
			},
			wantCalls: []call{
				{broker: 0, method: declareQueue, queue: "q"},
				{broker: 1, method: bindQueue, queue: "q"},
				{broker: 0, method: consumeQueue, queue: "q"},
			},
			wantErrs: []error{nil, nil, nil},
		},
		{
			name:     "each exclusive queue is pinned independently",
			haveErrs: []error{nil, nil},
			haveOps: []op{
				declare("q", true),
				declare("other", true),
				bind("q"),
				bind("other"),
			},
			wantCalls: []call{
				{broker: 0, method: declareQueue, queue: "q"},
				{broker: 1, method: declareQueue, queue: "other"},
				{broker: 0, method: bindQueue, queue: "q"},
				{broker: 1, method: bindQueue, queue: "other"},
			},
			wantErrs: []error{nil, nil, nil, nil},
		},
		{
			name:     "failed declare does not pin",
			haveErrs: []error{errFake, nil},
			haveOps: []op{
				declare("q", true),
				bind("q"),
			},
			wantCalls: []call{
				{broker: 0, method: declareQueue, queue: "q"},
				{broker: 1, method: bindQueue, queue: "q"},
			},
			wantErrs: []error{errFake, nil},
		},
		{
			name:     "failed operation on pinned broker keeps the pin",
			haveErrs: []error{nil, nil},
			haveOps: []op{
				declare("q", true),
				failWith(0, errFake),
				bind("q"),
				failWith(0, nil),
				consume("q"),
			},
			wantCalls: []call{
				{broker: 0, method: declareQueue, queue: "q"},
				{broker: 0, method: bindQueue, queue: "q"},
				{broker: 0, method: consumeQueue, queue: "q"},
			},
			wantErrs: []error{nil, nil, errFake, nil, nil},
		},
		{
			name:     "pin is dropped once the connection is closed",
			haveErrs: []error{nil, nil},
			haveOps: []op{
				declare("q", true),
				disconnect(0),
				declare("q", true),
				bind("q"),
			},
			wantCalls: []call{
				{broker: 0, method: declareQueue, queue: "q"},
				{broker: 1, method: declareQueue, queue: "q"},
				{broker: 1, method: bindQueue, queue: "q"},
			},
			wantErrs: []error{nil, nil, nil, nil},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var (
				ctx = context.Background()
				rec recorder
				bal roundRobinBalancer
			)

			for i, err := range tt.haveErrs {
				bal.peers = append(bal.peers, &fakeBroker{id: i, rec: &rec, err: err})
			}

			b := balancer.NewBroker(&bal, pbalancer.GetOptions{})

			errs := make([]error, 0, len(tt.haveOps))

			for _, o := range tt.haveOps {
				errs = append(errs, o(ctx, b, bal.peers))
			}

			require.Len(t, errs, len(tt.wantErrs))

			for i, err := range errs {
				require.ErrorIs(t, err, tt.wantErrs[i], "op %d", i)
			}

			assert.Equal(t, tt.wantCalls, rec.calls)
		})
	}
}

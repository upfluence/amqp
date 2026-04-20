package balancer_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/upfluence/amqp"
	"github.com/upfluence/amqp/backend/balancer"
	"github.com/upfluence/amqp/backend/rabbitmq"
)

// staticBroker is a minimal amqp.Broker stub that returns fixed BrokerStats.
type staticBroker struct {
	amqp.Broker
	stats rabbitmq.BrokerStats
}

func (b *staticBroker) Stats() rabbitmq.BrokerStats { return b.stats }

func wrap(stats rabbitmq.BrokerStats) balancer.BrokerWrapper {
	return balancer.BrokerWrapper{Broker: &staticBroker{stats: stats}}
}

func TestPicker(t *testing.T) {
	disconnected := func(idle, inUse int) rabbitmq.BrokerStats {
		return rabbitmq.BrokerStats{ConnectionOpened: false, IdleChannel: idle, InUseChannel: inUse}
	}
	connected := func(idle, inUse int) rabbitmq.BrokerStats {
		return rabbitmq.BrokerStats{ConnectionOpened: true, IdleChannel: idle, InUseChannel: inUse}
	}

	for name, tc := range map[string]struct {
		peers   []balancer.BrokerWrapper
		wantIdx []int
		wantErr error
	}{
		"no peers returns error": {
			peers:   nil,
			wantErr: balancer.ErrNoBrokerAvailable,
		},
		"phase 1 – prefers connected broker with idle channel": {
			peers: []balancer.BrokerWrapper{
				wrap(disconnected(2, 0)),
				wrap(connected(1, 5)),
			},
			wantIdx: []int{1},
		},
		"phase 1 – among connected+idle, picks highest InUse": {
			peers: []balancer.BrokerWrapper{
				wrap(connected(1, 3)),
				wrap(connected(1, 10)),
				wrap(connected(1, 7)),
			},
			wantIdx: []int{1},
		},
		"phase 2 – falls back to connected broker when none has idle channel": {
			peers: []balancer.BrokerWrapper{
				wrap(disconnected(0, 0)),
				wrap(connected(0, 5)),
				wrap(connected(0, 2)),
			},
			wantIdx: []int{1},
		},
		"phase 3 – round-robins when no broker is connected": {
			peers: []balancer.BrokerWrapper{
				wrap(disconnected(0, 0)),
				wrap(disconnected(0, 0)),
				wrap(disconnected(0, 0)),
			},
			wantIdx: []int{0, 1, 2, 0, 1, 2},
		},
	} {
		t.Run(name, func(t *testing.T) {
			p := &balancer.Picker{}
			ctx := context.Background()

			if tc.wantErr != nil {
				_, err := p.Pick(ctx, tc.peers)
				require.ErrorIs(t, err, tc.wantErr)

				return
			}

			for i, want := range tc.wantIdx {
				got, err := p.Pick(ctx, tc.peers)
				require.NoError(t, err, "call %d", i)
				assert.Equal(t, tc.peers[want], got, "call %d", i)
			}
		})
	}
}

package balancer_test

import (
	"context"
	"slices"
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
		// wantCycle lists the brokers picked in turn, starting from any of
		// them since the round-robin starts at a random position.
		wantCycle []int
		wantErr   error
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
		"phase 2 – round-robins across connected brokers when none has idle channel": {
			peers: []balancer.BrokerWrapper{
				wrap(disconnected(0, 0)),
				wrap(connected(0, 5)),
				wrap(connected(0, 2)),
			},
			wantCycle: []int{1, 2},
		},
		"phase 3 – round-robins when no broker is connected": {
			peers: []balancer.BrokerWrapper{
				wrap(disconnected(0, 0)),
				wrap(disconnected(0, 0)),
				wrap(disconnected(0, 0)),
			},
			wantCycle: []int{0, 1, 2},
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

			if len(tc.wantCycle) == 0 {
				return
			}

			first, err := p.Pick(ctx, tc.peers)
			require.NoError(t, err)

			start := slices.IndexFunc(tc.wantCycle, func(i int) bool { return tc.peers[i] == first })
			require.GreaterOrEqual(t, start, 0, "picked a broker out of the cycle")

			for i := 1; i <= 2*len(tc.wantCycle); i++ {
				got, err := p.Pick(ctx, tc.peers)
				require.NoError(t, err, "call %d", i)

				want := tc.wantCycle[(start+i)%len(tc.wantCycle)]
				assert.Equal(t, tc.peers[want], got, "call %d", i)
			}
		})
	}
}

func TestPickerRandomStart(t *testing.T) {
	var (
		ctx   = context.Background()
		peers = []balancer.BrokerWrapper{
			wrap(rabbitmq.BrokerStats{}),
			wrap(rabbitmq.BrokerStats{}),
			wrap(rabbitmq.BrokerStats{}),
		}
		picked = make(map[int]int)
	)

	// Each process builds its own Picker over the same list of brokers: their
	// first connection must not always go to the same one. The odds of a
	// broker never being picked first by 100 pickers are (2/3)^100.
	for range 100 {
		got, err := (&balancer.Picker{}).Pick(ctx, peers)
		require.NoError(t, err)

		picked[slices.IndexFunc(peers, func(p balancer.BrokerWrapper) bool { return p == got })]++
	}

	assert.Len(t, picked, len(peers))
}

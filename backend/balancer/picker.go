package balancer

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"

	"github.com/upfluence/errors"
	"github.com/upfluence/pkg/v2/discovery/balancer/simple"

	"github.com/upfluence/amqp/backend/rabbitmq"
)

// ErrNoBrokerAvailable is returned when no broker can be selected.
var ErrNoBrokerAvailable = errors.New("balancer/picker: no broker available")

// Picker implements simple.Picker[BrokerWrapper].
//
// Selection strategy (in order of preference):
//  1. Brokers with an open connection are always preferred over those without.
//  2. Among brokers with an open connection, prefer the one that is most
//     loaded (highest InUseChannel count) while still having at least one
//     idle channel available — this packs work onto already-warm brokers
//     and avoids opening new connections unnecessarily.
//  3. If no broker has both a connection and an idle channel (all are fully
//     saturated), round-robin across the connected brokers.
//  4. If no broker has an open connection at all, round-robin across all
//     candidates so that connection attempts are spread evenly.
//
// The round-robin starts at a random position, so that processes sharing the
// same list of brokers do not all open their first connection to the same
// one.
//
// Picker is safe for concurrent use. Its zero value is ready to use.
type Picker struct {
	once  sync.Once
	index atomic.Uint64
}

var _ simple.Picker[BrokerWrapper] = (*Picker)(nil)

func (p *Picker) next() uint64 {
	p.once.Do(func() { p.index.Store(rand.Uint64()) }) //nolint:gosec

	return p.index.Add(1) - 1
}

func (p *Picker) Pick(_ context.Context, peers []BrokerWrapper) (BrokerWrapper, error) {
	if len(peers) == 0 {
		var zero BrokerWrapper

		return zero, ErrNoBrokerAvailable
	}

	// Gather stats once so we don't call GetStats multiple times per peer.
	type candidate struct {
		bw    BrokerWrapper
		stats rabbitmq.BrokerStats
	}

	candidates := make([]candidate, len(peers))
	for i, bw := range peers {
		candidates[i] = candidate{bw: bw, stats: rabbitmq.GetStats(bw.Broker)}
	}

	// Phase 1: connected brokers that have at least one idle channel.
	// Among these, pick the one with the highest InUseChannel count (most
	// loaded but still able to serve without creating a new channel).
	bestIdx := -1
	bestInUse := -1

	for i, c := range candidates {
		if !c.stats.ConnectionOpened || c.stats.IdleChannel < 1 {
			continue
		}

		if c.stats.InUseChannel > bestInUse {
			bestInUse = c.stats.InUseChannel
			bestIdx = i
		}
	}

	if bestIdx >= 0 {
		return candidates[bestIdx].bw, nil
	}

	// Phase 2: connected brokers but none with an idle channel (all fully
	// saturated). Round-robin across them; at this point all have zero idle
	// channels so there is no meaningful tiebreak.
	var connected []BrokerWrapper

	for _, c := range candidates {
		if c.stats.ConnectionOpened {
			connected = append(connected, c.bw)
		}
	}

	if len(connected) > 0 {
		return connected[p.next()%uint64(len(connected))], nil
	}

	// Phase 3: no broker has an open connection; round-robin across all
	// candidates so that connection attempts are spread evenly.
	return candidates[p.next()%uint64(len(candidates))].bw, nil
}

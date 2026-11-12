package amqputil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/upfluence/pkg/v2/peer"
	"github.com/upfluence/pkg/v2/peer/version"
)

func TestPeerTable(t *testing.T) {
	const (
		app  = "magento-provider"
		unit = "backfiller-private-deployment-89fc677db-2rkp6"
		name = "peer://qa@global-2023.eks.upfluence.co/" + unit + "?app-name=" + app + "&git-version=bf46b475381e"
	)

	for _, tt := range []struct {
		name     string
		havePeer *peer.Peer
		want     map[string]any
	}{
		{
			name: "nil peer",
		},
		{
			name: "peer",
			havePeer: &peer.Peer{
				Authority:    "global-2023.eks.upfluence.co",
				InstanceName: unit,
				AppName:      app,
				ProjectName:  app,
				Environment:  "qa",
				Version: version.Version{
					Git: version.GitVersion{Commit: "bf46b475381e"},
				},
			},
			want: map[string]any{
				"connection_name":           name,
				"upfluence-connection-name": name,
				"upfluence-unit-name":       unit,
				"upfluence-app-name":        app,
				"upfluence-project-name":    app,
				"upfluence-env":             "qa",
				"upfluence-version":         "v.0.0.0+git-bf46b47",
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, peerTable(tt.havePeer))
		})
	}
}

package rabbitmq

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsumerTag(t *testing.T) {
	for _, tc := range []struct {
		name     string
		haveTag  string
		wantTag  string
		assertFn func(*testing.T, string)
	}{
		{
			name:    "keeps explicit tag",
			haveTag: "consumer-tag",
			wantTag: "consumer-tag",
		},
		{
			name: "generates empty tag",
			assertFn: func(t *testing.T, got string) {
				t.Helper()

				require.NotEmpty(t, got)
				assert.True(t, strings.HasPrefix(got, "upfluence-amqp-"))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := consumerTag(tc.haveTag)

			if tc.assertFn != nil {
				tc.assertFn(t, got)

				return
			}

			assert.Equal(t, tc.wantTag, got)
		})
	}
}

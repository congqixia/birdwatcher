package kv

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
)

// TestEtcdLoadWithPrefixPagination verifies that LoadWithPrefix paginates over
// multiple Range pages when the result set exceeds the page size, returning the
// complete, sorted listing instead of silently dropping trailing keys.
func TestEtcdLoadWithPrefixPagination(t *testing.T) {
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "warn"
	cfg.LogOutputs = []string{"default"}
	u, err := url.Parse("http://localhost:0")
	require.NoError(t, err)
	cfg.ListenClientUrls = []url.URL{*u}
	cfg.ListenPeerUrls = []url.URL{*u}
	e, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	defer e.Close()
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(60 * time.Second):
		t.Fatal("embed etcd not ready")
	}

	cli := v3client.New(e.Server)
	kv := NewEtcdKV(cli)
	defer kv.Close()

	ctx := context.TODO()
	const total = 2000 // far larger than the 100-entry page size, forcing multi-page
	for i := 0; i < total; i++ {
		require.NoError(t, kv.Save(ctx, fmt.Sprintf("prefix/k%04d", i), strings.Repeat("v", 128)))
	}

	// 1) Completeness and order: every key exactly once, strictly increasing.
	keys, values, err := kv.LoadWithPrefix(ctx, "prefix")
	require.NoError(t, err)
	require.Len(t, keys, total)
	require.Len(t, values, total)
	for i := 0; i < total; i++ {
		require.Equal(t, fmt.Sprintf("prefix/k%04d", i), keys[i], "keys must be complete and strictly increasing at %d", i)
		require.Equal(t, strings.Repeat("v", 128), values[i])
	}

	// 2) WithKeysOnly: keys complete, values all empty.
	keys, values, err = kv.LoadWithPrefix(ctx, "prefix", WithKeysOnly())
	require.NoError(t, err)
	require.Len(t, keys, total)
	for _, v := range values {
		require.Empty(t, v)
	}

	// 3) Empty prefix: full keyspace listing — etcd rejects an empty range
	// start key, so the empty-prefix scan must start at "\x00" instead.
	keys, values, err = kv.LoadWithPrefix(ctx, "")
	require.NoError(t, err)
	require.Len(t, keys, total)
	for i := 0; i < total; i++ {
		require.Equal(t, fmt.Sprintf("prefix/k%04d", i), keys[i])
		require.Equal(t, strings.Repeat("v", 128), values[i])
	}

	// 4) Unmatched prefix: empty result, no error.
	keys, values, err = kv.LoadWithPrefix(ctx, "does-not-exist")
	require.NoError(t, err)
	require.Empty(t, keys)
	require.Empty(t, values)
}

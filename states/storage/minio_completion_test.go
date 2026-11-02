package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/oss"
)

type completionStore struct {
	oss.ObjectStore
	calls int
}

func (s *completionStore) List(_ context.Context, prefix string, _ bool) (<-chan oss.ObjectInfo, error) {
	s.calls++
	ch := make(chan oss.ObjectInfo, 1)
	ch <- oss.ObjectInfo{Key: prefix + "folder/"}
	close(ch)
	return ch, nil
}

func TestOSSPathCompletionConfig(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "birdwatcher.yaml"), []byte("WorkspacePath: "+dir+"\n"), 0o600))
	cfg, err := configs.NewConfig(dir)
	require.NoError(t, err)
	t.Setenv(configs.OSSPathCompletion, "")
	store := &completionStore{}
	state := &OSSState{CmdState: framework.NewCmdState("test", cfg), store: store, config: cfg}

	require.Equal(t, []string{"folder"}, state.suggestPaths(""))
	require.Equal(t, 1, store.calls)
	require.NoError(t, cfg.SetConfig("env", configs.OSSPathCompletion, "false"))
	require.Empty(t, state.suggestPaths(""))
	require.Equal(t, 1, store.calls, "disabled completion must not list objects")
	require.NoError(t, state.CdCommand(context.Background(), &CdParam{prefix: "folder"}))
	require.Equal(t, "folder", state.prefix)
	require.Equal(t, 2, store.calls, "cd must still validate paths")
	require.NoError(t, cfg.SetConfig("env", configs.OSSPathCompletion, "true"))
	require.Equal(t, []string{"folder"}, state.suggestPaths(""))
	require.Equal(t, 3, store.calls)
}

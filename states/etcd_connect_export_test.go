package states

import (
	"context"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
	metakv "github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/birdwatcher/states/storage"
)

func TestGetKVConnectedStateReturnsMetastoreState(t *testing.T) {
	config := &configs.Config{}
	core := framework.NewCmdState("[core]", config)
	client := metakv.NewFileAuditKV(nil, nil)

	state := GetKVConnectedState(core, client, "scout:2379", config, nil, nil)
	require.NotNil(t, state)
	require.Contains(t, state.Label(), "MetaStore(scout:2379)")
}

func TestDefaultMetaPathIsMeta(t *testing.T) {
	require.Equal(t, "meta", DefaultMetaPath)
}

func TestPingMetaStoreIsExported(t *testing.T) {
	require.NotNil(t, PingMetaStore)
}

func TestExportedStateTags(t *testing.T) {
	require.Equal(t, "etcd", EtcdTag)
	require.Equal(t, "tikv", TiKVTag)
	require.Equal(t, "pulsar", PulsarTag)
	require.Equal(t, "oss", OSSTag)
}

func TestConnectParamsRootPathProvided(t *testing.T) {
	t.Run("flag not set", func(t *testing.T) {
		cp := &ConnectParams{}
		fs := pflag.NewFlagSet("connect", pflag.ContinueOnError)
		fs.String("rootPath", "by-dev", "")
		require.NoError(t, fs.Parse(nil))
		cp.SetFlagSet(fs)
		require.False(t, cp.rootPathProvided())
	})

	t.Run("flag set explicitly", func(t *testing.T) {
		cp := &ConnectParams{}
		fs := pflag.NewFlagSet("connect", pflag.ContinueOnError)
		fs.String("rootPath", "by-dev", "")
		require.NoError(t, fs.Parse([]string{"--rootPath", "tenant-a"}))
		cp.SetFlagSet(fs)
		require.True(t, cp.rootPathProvided())
	})

	t.Run("env provided", func(t *testing.T) {
		cp := &ConnectParams{rootPathFromEnv: true}
		require.True(t, cp.rootPathProvided())
	})

	t.Run("no flagset", func(t *testing.T) {
		cp := &ConnectParams{}
		require.False(t, cp.rootPathProvided())
	})
}

type connectFlagReceiver struct{}

func (r *connectFlagReceiver) ConnectCommand(_ context.Context, _ *ConnectParams) error {
	return nil
}

func TestConnectCommandRedactsEtcdPassword(t *testing.T) {
	host := &struct{ *framework.CmdState }{framework.NewCmdState("test", &configs.Config{})}
	root := &cobra.Command{}
	host.MergeFunctionCommandsFrom(root, host, &connectFlagReceiver{})

	require.Equal(t,
		"connect --etcd 127.0.0.1:2379 --etcdUserName root --etcdPassword [REDACTED]",
		framework.RedactCommand("connect --etcd 127.0.0.1:2379 --etcdUserName root --etcdPassword s3cret"))
	require.Equal(t,
		"connect --etcdPassword=[REDACTED]",
		framework.RedactCommand("connect --etcdPassword=s3cret"))
}

type connectOSSFlagReceiver struct{}

func (r *connectOSSFlagReceiver) ConnectMinioCommand(_ context.Context, _ *storage.ConnectOSSParam) error {
	return nil
}

func TestConnectOSSCommandRedactsKeys(t *testing.T) {
	host := &struct{ *framework.CmdState }{framework.NewCmdState("test", &configs.Config{})}
	root := &cobra.Command{}
	host.MergeFunctionCommandsFrom(root, host, &connectOSSFlagReceiver{})

	require.Equal(t,
		"connect oss --bucket a-bucket --ak [REDACTED] --sk [REDACTED]",
		framework.RedactCommand("connect oss --bucket a-bucket --ak minioadmin --sk s3cret"))
	require.Equal(t,
		"connect oss --ak=[REDACTED] --sk=[REDACTED]",
		framework.RedactCommand("connect oss --ak=minioadmin --sk=s3cret"))
}

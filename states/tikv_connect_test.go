package states

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/kv"
)

func TestTiKVConnectTimeout(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  time.Duration
	}{
		{"", 5 * time.Second},
		{"5s", 5 * time.Second},
		{"250ms", 250 * time.Millisecond},
	} {
		t.Run(tc.value, func(t *testing.T) {
			cp := &ConnectParams{TiKVConnectTimeout: tc.value}
			start := time.Now()
			ctx, cancel, err := cp.tikvConnectContext(context.Background())
			require.NoError(t, err)
			defer cancel()
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.WithinDuration(t, start.Add(tc.want), deadline, 50*time.Millisecond)
		})
	}
	for _, value := range []string{"0", "-1s", "bad", "5"} {
		t.Run("invalid_"+value, func(t *testing.T) {
			app := &ApplicationState{}
			err := app.ConnectCommand(context.Background(), &ConnectParams{
				UseTiKV: true, TiKVAddr: "127.0.0.1:0", TiKVConnectTimeout: value,
			})
			require.ErrorContains(t, err, "tikvConnectTimeout must be a positive duration")
		})
	}
	t.Run("earlier parent deadline", func(t *testing.T) {
		parent, parentCancel := context.WithTimeout(context.Background(), time.Second)
		defer parentCancel()
		ctx, cancel, err := (&ConnectParams{}).tikvConnectContext(parent)
		require.NoError(t, err)
		defer cancel()
		want, _ := parent.Deadline()
		got, _ := ctx.Deadline()
		require.Equal(t, want, got)
	})
}

func TestTiKVConnectTimeoutFlag(t *testing.T) {
	host := &struct{ *framework.CmdState }{framework.NewCmdState("test", &configs.Config{})}
	root := &cobra.Command{}
	host.MergeFunctionCommandsFrom(root, host, &connectFlagReceiver{})
	command, _, err := root.Find([]string{"connect"})
	require.NoError(t, err)
	flag := command.Flags().Lookup("tikvConnectTimeout")
	require.NotNil(t, flag)
	require.Equal(t, "5s", flag.DefValue)
	require.NoError(t, command.Flags().Parse([]string{"--tikvConnectTimeout", "250ms"}))
	require.Equal(t, "250ms", flag.Value.String())
}

type connectTestKV struct {
	kv.MetaKV
	load   func(context.Context, string) (string, error)
	closed bool
}

func (c *connectTestKV) Load(ctx context.Context, key string, _ ...kv.LoadOption) (string, error) {
	return c.load(ctx, key)
}

func (c *connectTestKV) Close() { c.closed = true }

func TestTiKVMetadataFailureClosesClient(t *testing.T) {
	cfg := &configs.Config{}
	app := &ApplicationState{
		core: framework.NewCmdState("test", cfg), config: cfg, states: map[string]framework.State{},
	}
	for _, waitForDeadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "read error", true: "deadline"}[waitForDeadline], func(t *testing.T) {
			errRead := errors.New("simulated TiKV store unavailable")
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			deadline, _ := ctx.Deadline()
			cli := &connectTestKV{load: func(ctx context.Context, key string) (string, error) {
				got, _ := ctx.Deadline()
				require.Equal(t, deadline, got) // The initialization budget must not restart here.
				require.Equal(t, "test/meta/session/id", key)
				if waitForDeadline {
					<-ctx.Done()
					return "", ctx.Err()
				}
				return "", errRead
			}}
			err := app.finishTiKVConnect(ctx, &ConnectParams{TiKVAddr: "127.0.0.1:2379", RootPath: "test", MetaPath: "meta"}, cli)
			if waitForDeadline {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			} else {
				require.ErrorIs(t, err, errRead)
			}
			require.ErrorContains(t, err, "TiKV metadata read")
			require.True(t, cli.closed)
			require.Empty(t, app.states)
		})
	}
}

func TestTiKVDryConnectOwnsClient(t *testing.T) {
	cfg := &configs.Config{}
	app := &ApplicationState{
		core: framework.NewCmdState("test", cfg), config: cfg, states: map[string]framework.State{},
	}
	cli := &connectTestKV{load: func(context.Context, string) (string, error) {
		t.Fatal("dry mode must not read metadata")
		return "", nil
	}}
	require.NoError(t, app.finishTiKVConnect(context.Background(), &ConnectParams{Dry: true}, cli))
	require.False(t, cli.closed)
	require.Contains(t, app.states, tikvTag)
	app.Close()
	require.True(t, cli.closed)
}

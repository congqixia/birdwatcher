package states

import (
	"context"
	"testing"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestOSSOverrides(t *testing.T) {
	original := oss.MinioClientParam{UseIAM: true, UseSSL: true, AK: "old-ak", SK: "old-sk", Region: "original"}
	params, err := (OSSOverrideParam{}).connectParams()
	require.NoError(t, err)
	got := original
	for _, apply := range params {
		apply(&got)
	}
	require.Equal(t, original, got)
	p := OSSOverrideParam{UseIAM: "false", UseSSL: "false", AccessKeyID: "new-ak", CloudProvider: "aws", BucketName: "bucket", RootPath: "root", IAMEndpoint: "iam", LoadFrequency: "60"}
	params, err = p.connectParams()
	require.NoError(t, err)
	for _, apply := range params {
		apply(&got)
	}
	require.False(t, got.UseIAM)
	require.False(t, got.UseSSL)
	require.Equal(t, "new-ak", got.AK)
	require.Equal(t, "old-sk", got.SK)
	require.Equal(t, "original", got.Region)
	require.Equal(t, "aws", got.CloudProvider)
	require.Equal(t, "bucket", got.BucketName)
	require.Equal(t, "root", got.RootPath)
	require.Equal(t, "iam", got.IAMEndpoint)
	require.Equal(t, 60, got.LoadFrequency)
	for _, p := range []OSSOverrideParam{{UseIAM: "masked"}, {UseSSL: "bad"}, {LoadFrequency: "-1"}, {LoadFrequency: "masked"}} {
		_, err := p.connectParams()
		require.Error(t, err)
	}
}

func TestOSSStandaloneRequiresLocation(t *testing.T) {
	// A nil state ensures standalone never attempts configuration discovery.
	var state *InstanceState
	_, err := state.getObjectStoreWithOverrides(context.Background(), OSSOverrideParam{OSSStandalone: true}, "", true)
	require.ErrorContains(t, err, "requires --minioAddr")
}

type footerFlagReceiver struct{ param *ShowParquetFooterParam }

func (r *footerFlagReceiver) FooterCommand(_ context.Context, p *ShowParquetFooterParam) error {
	r.param = p
	return nil
}
func TestParquetFooterOverrideFlags(t *testing.T) {
	host := &struct{ *framework.CmdState }{framework.NewCmdState("test", &configs.Config{})}
	receiver := &footerFlagReceiver{}
	root := &cobra.Command{SilenceUsage: true, SilenceErrors: true}
	host.MergeFunctionCommandsFrom(root, host, receiver)
	root.SetArgs([]string{"show", "parquet-footer", "--collection", "123", "--top", "7", "--ossStandalone", "--useIAM", "true", "--useSSL", "false", "--cloudProvider", "aws", "--bucketName", "bucket", "--minioAddr", "localhost:9000"})
	require.NoError(t, root.Execute())
	require.NotNil(t, receiver.param)
	require.Equal(t, int64(7), receiver.param.Top)
	require.Equal(t, "true", receiver.param.UseIAM)
	require.Equal(t, "false", receiver.param.UseSSL)
	require.Equal(t, "aws", receiver.param.CloudProvider)
	require.True(t, receiver.param.OSSStandalone)
}

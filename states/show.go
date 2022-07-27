package states

import (
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// getEtcdShowCmd returns sub command for instanceState
// show [subCommand] [options...]
//  sub command [collection|session|segment]
func getEtcdShowCmd(cli *clientv3.Client, basePath string) *cobra.Command {
	showCmd := &cobra.Command{
		Use: "show",
	}

	showCmd.AddCommand(
		getEtcdShowCollection(cli, basePath),
		getEtcdShowSession(cli, basePath),
		getEtcdShowSegments(cli, basePath),
		getLoadedSegmentsCmd(cli, basePath),
		getEtcdShowReplica(cli, basePath),
	)
	return showCmd
}

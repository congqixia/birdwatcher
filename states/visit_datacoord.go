package states

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/milvus-io/birdwatcher/common"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	datapbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/datapb"
	milvuspbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/milvuspb"
)

type dataCoordState struct {
	common.CmdState
	session   *models.Session
	client    datapb.DataCoordClient
	clientv2  datapbv2.DataCoordClient
	conn      *grpc.ClientConn
	prevState common.State
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *dataCoordState) SetupCommands() {
	cmd := &cobra.Command{}
	cmd.AddCommand(
		// metrics
		getMetricsCmd(s.client),
		// configuration
		getConfigurationCmd(s.clientv2, s.session.ServerID),
		// back
		getBackCmd(s, s.prevState),

		// compact
		compactCmd(s.clientv2),
		// flush
		flushCommand(s.clientv2),

		// exit
		getExitCmd(s),
	)

	s.MergeFunctionCommands(cmd, s)

	s.CmdState.RootCmd = cmd
	s.SetupFn = s.SetupCommands
}

func getDataCoordState(client datapb.DataCoordClient, conn *grpc.ClientConn, prev common.State, session *models.Session) common.State {
	state := &dataCoordState{
		CmdState: common.CmdState{
			LabelStr: fmt.Sprintf("DataCoord-%d(%s)", session.ServerID, session.Address),
		},
		session:   session,
		client:    client,
		clientv2:  datapbv2.NewDataCoordClient(conn),
		conn:      conn,
		prevState: prev,
	}

	state.SetupCommands()

	return state
}

func compactCmd(client datapbv2.DataCoordClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "compact",
		Short:   "manual compact with collectionID",
		Aliases: []string{"manualCompact"},
		Run: func(cmd *cobra.Command, args []string) {
			collectionID, err := cmd.Flags().GetInt64("collectionID")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			resp, err := client.ManualCompaction(ctx, &milvuspbv2.ManualCompactionRequest{
				CollectionID: collectionID,
			})
			if err != nil {
				fmt.Printf("manual compact fail with collectionID:%d, error: %s", collectionID, err.Error())
				return
			}
			fmt.Printf("manual compact done, collectionID:%d, compactionID:%d, rpc status:%v",
				collectionID, resp.GetCompactionID(), resp.GetStatus())
		},
	}

	cmd.Flags().Int64("collectionID", -1, "compact with collectionID")
	return cmd
}

func flushCommand(client datapbv2.DataCoordClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "flush",
		Short: "manual flush with collectionID",
		Run: func(cmd *cobra.Command, args []string) {
			collectionID, err := cmd.Flags().GetInt64("collectionID")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			resp, err := client.Flush(ctx, &datapbv2.FlushRequest{
				CollectionID: collectionID,
			})
			if err != nil {
				fmt.Printf("manual flush fail with collectionID:%d, error: %s", collectionID, err.Error())
				return
			}
			fmt.Printf("manual flush done, collectionID:%d, rpc status:%v",
				collectionID, resp.GetStatus())
		},
	}

	cmd.Flags().Int64("collectionID", -1, "compact with collectionID")
	return cmd
}

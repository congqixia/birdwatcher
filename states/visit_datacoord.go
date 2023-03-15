package states

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	datapbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/datapb"
	milvuspbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/milvuspb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

type dataCoordState struct {
	cmdState
	session   *models.Session
	client    datapb.DataCoordClient
	clientv2  datapbv2.DataCoordClient
	conn      *grpc.ClientConn
	prevState State
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
		//back
		getBackCmd(s, s.prevState),
		// compact
		getCompactCmd(s.clientv2, s.session.ServerID),
		// exit
		getExitCmd(s),
	)
	cmd.AddCommand(getGlobalUtilCommands()...)

	s.cmdState.rootCmd = cmd
	s.setupFn = s.SetupCommands
}

func getDataCoordState(client datapb.DataCoordClient, conn *grpc.ClientConn, prev State, session *models.Session) State {

	state := &dataCoordState{
		cmdState: cmdState{
			label: fmt.Sprintf("DataCoord-%d(%s)", session.ServerID, session.Address),
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

func getCompactCmd(clientv2 datapbv2.DataCoordClient, id int64) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "compact",
		Short: "trigger compaction",
		Run: func(cmd *cobra.Command, args []string) {
			collectionID, err := cmd.Flags().GetInt64("collection")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			req := &milvuspbv2.ManualCompactionRequest{
				CollectionID: collectionID,
			}

			resp, err := clientv2.ManualCompaction(context.Background(), req)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			fmt.Printf("trigger compaction %s\n", resp.GetStatus().GetErrorCode().String())
		},
	}

	cmd.Flags().Int64("collection", 0, "collection id")
	return cmd

}

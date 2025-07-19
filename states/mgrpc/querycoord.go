package mgrpc

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

type queryCoordState struct {
	*framework.CmdState
	session   *models.Session
	client    querypb.QueryCoordClient
	conn      *grpc.ClientConn
	prevState framework.State
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *queryCoordState) SetupCommands() {
	cmd := s.GetCmd()

	s.UpdateState(cmd, s, s.SetupCommands)
}

type BalanceSegmentParam struct {
	framework.ParamBase `use:"balance-segment" desc:"balance segment"`
	CollectionID        int64   `name:"collection" default:"0"`
	SegmentIDs          []int64 `name:"segment" desc:"segment ids to balance"`
	SourceNodes         []int64 `name:"srcNodes" desc:"from querynode ids"`
	DstNodes            int64   `name:"dstNode" desc:"to querynode ids"`
}

func (s *queryCoordState) BalanceSegmentCommand(ctx context.Context, p *BalanceSegmentParam) error {
	req := &querypb.LoadBalanceRequest{
		Base: &commonpb.MsgBase{
			TargetID: s.session.ServerID,
		},
		CollectionID:     p.CollectionID,
		SealedSegmentIDs: p.SegmentIDs,
		SourceNodeIDs:    p.SourceNodes,
	}

	resp, err := s.client.LoadBalance(ctx, req)
	if err != nil {
		return err
	}
	fmt.Println(resp)
	return nil
}

type LoadParam struct {
	framework.ParamBase `use:"balance-segment" desc:"balance segment"`
	CollectionID        int64 `name:"collection" default:"0"`
	Refresh             bool  `name:"refresh" default:"false" desc:"whether to refresh the collection"`
}

func (s *queryCoordState) LoadCommand(ctx context.Context, p *LoadParam) error {
	req := &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			TargetID: s.session.ServerID,
			MsgType:  commonpb.MsgType_LoadCollection,
		},
		CollectionID: p.CollectionID,
		Refresh:      p.Refresh,
	}

	resp, err := s.client.LoadCollection(ctx, req)
	if err != nil {
		return err
	}
	fmt.Println(resp)
	return nil
}

/*
func (s *queryCoordState) ShowCollectionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use: "show-collection",
		Run: func(cmd *cobra.Command, args []string) {
			collection, err := cmd.Flags().GetInt64("collection")
			if err != nil {
				cmd.Usage()
				return
			}

			req := &querypb.ShowCollectionsRequest{
				Base: &commonpb.MsgBase{
					TargetID: s.session.ServerID,
				},
				CollectionIDs: []int64{collection},
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			resp, err := s.clientv2.ShowCollections(ctx, req)
			if err != nil {
				fmt.Println(err.Error())
			}

			fmt.Printf("%s, %s", resp.GetStatus().GetErrorCode().String(), resp.GetStatus().GetReason())
		},
	}

	cmd.Flags().Int64("collection", 0, "collection to show")
	return cmd
}*/

func GetQueryCoordState(client querypb.QueryCoordClient, conn *grpc.ClientConn, prev *framework.CmdState, session *models.Session) framework.State {
	state := &queryCoordState{
		CmdState:  prev.Spawn(fmt.Sprintf("QueryCoord-%d(%s)", session.ServerID, session.Address)),
		session:   session,
		client:    client,
		conn:      conn,
		prevState: prev,
	}

	state.SetupCommands()

	return state
}

func checkerActivationCmd(clientv2 querypb.QueryCoordClient, id int64) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "checker",
		Short: "checker cmd",
	}
	cmd.AddCommand(
		checkerActivateCmd(clientv2, id),
		checkerDeactivateCmd(clientv2, id),
		checkerListCmd(clientv2, id),
	)
	return cmd
}

func checkerActivateCmd(clientv2 querypb.QueryCoordClient, id int64) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "activate",
		Short: "activate checkerID",
		Run: func(cmd *cobra.Command, args []string) {
			checkerID, err := strconv.ParseInt(args[0], 10, 64)
			if err != nil {
				fmt.Println("checkerID must be a number")
				return
			}
			req := &querypb.ActivateCheckerRequest{
				Base: &commonpb.MsgBase{
					TargetID: id,
					SourceID: -1,
				},
				CheckerID: int32(checkerID),
			}

			status, err := clientv2.ActivateChecker(context.Background(), req)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			if status.ErrorCode != commonpb.ErrorCode_Success {
				fmt.Print(status.Reason)
				return
			}
			fmt.Println("success")
		},
	}

	return cmd
}

func checkerDeactivateCmd(clientv2 querypb.QueryCoordClient, id int64) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "deactivate",
		Short: "deactivate checkerID",
		Run: func(cmd *cobra.Command, args []string) {
			checkerID, err := strconv.ParseInt(args[0], 10, 64)
			if err != nil {
				fmt.Println("checkerID must be a number")
				return
			}
			req := &querypb.DeactivateCheckerRequest{
				Base: &commonpb.MsgBase{
					TargetID: id,
					SourceID: -1,
				},
				CheckerID: int32(checkerID),
			}

			status, err := clientv2.DeactivateChecker(context.Background(), req)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			if status.ErrorCode != commonpb.ErrorCode_Success {
				fmt.Print(status.Reason)
				return
			}
			fmt.Println("success")
		},
	}

	return cmd
}

func checkerListCmd(clientv2 querypb.QueryCoordClient, id int64) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "checker list [checkerIDs]",
		Run: func(cmd *cobra.Command, args []string) {
			checkerIDs := make([]int32, 0)
			for _, arg := range args {
				checkerID, err := strconv.ParseInt(arg, 10, 32)
				if err != nil {
					fmt.Println("checkerID must be number")
				}
				checkerIDs = append(checkerIDs, int32(checkerID))
			}

			req := &querypb.ListCheckersRequest{
				Base: &commonpb.MsgBase{
					TargetID: id,
					SourceID: -1,
				},
				CheckerIDs: checkerIDs,
			}

			resp, err := clientv2.ListCheckers(context.Background(), req)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
				fmt.Println(resp.Status.Reason)
				return
			}

			sort.Slice(resp.CheckerInfos, func(i, j int) bool {
				return resp.CheckerInfos[i].GetId() < resp.CheckerInfos[j].GetId()
			})
			w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', tabwriter.AlignRight|tabwriter.Debug)
			fmt.Fprintln(w, "id\tdesc\tfound\tactivated")
			for _, info := range resp.CheckerInfos {
				fmt.Fprintf(w, "%v\t%v\t%v\t%v\n", info.GetId(), info.GetDesc(), info.GetFound(), info.GetActivated())
			}
			w.Flush()
		},
	}
	return cmd
}

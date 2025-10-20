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

// ===== New Ops Commands =====

// QueryNode Management commands
type ListQueryNodeParam struct {
	framework.ParamBase `use:"list-querynodes" desc:"List all QueryNode instances"`
}

func (s *queryCoordState) ListQueryNodeCommand(ctx context.Context, p *ListQueryNodeParam) error {
	req := &querypb.ListQueryNodeRequest{
		Base: &commonpb.MsgBase{
			TargetID: s.session.ServerID,
			SourceID: -1,
		},
	}

	resp, err := s.client.ListQueryNode(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to list query nodes: %w", err)
	}
	if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
		return fmt.Errorf("list query nodes failed: %s", resp.Status.Reason)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "Node ID\tAddress\tState")
	fmt.Fprintln(w, "---\t---\t---")
	for _, node := range resp.NodeInfos {
		fmt.Fprintf(w, "%d\t%s\t%s\n", node.ID, node.Address, node.State)
	}
	w.Flush()

	fmt.Printf("\nTotal nodes: %d\n", len(resp.NodeInfos))
	return nil
}

type GetQueryNodeDistributionParam struct {
	framework.ParamBase `use:"get-querynode-distribution" desc:"Get data distribution for a specific QueryNode"`
	NodeID              int64 `name:"nodeID" desc:"QueryNode ID to query"`
}

func (s *queryCoordState) GetQueryNodeDistributionCommand(ctx context.Context, p *GetQueryNodeDistributionParam) error {
	req := &querypb.GetQueryNodeDistributionRequest{
		Base: &commonpb.MsgBase{
			TargetID: s.session.ServerID,
			SourceID: -1,
		},
		NodeID: p.NodeID,
	}

	resp, err := s.client.GetQueryNodeDistribution(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to get query node distribution: %w", err)
	}
	if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
		return fmt.Errorf("get query node distribution failed: %s", resp.Status.Reason)
	}

	fmt.Printf("QueryNode %d Distribution:\n", p.NodeID)
	fmt.Printf("Channels (%d): %v\n", len(resp.ChannelNames), resp.ChannelNames)
	fmt.Printf("Sealed Segments (%d): %v\n", len(resp.SealedSegmentIDs), resp.SealedSegmentIDs)
	return nil
}

type SuspendNodeParam struct {
	framework.ParamBase `use:"suspend-node" desc:"Suspend a QueryNode from resource operations"`
	NodeID              int64 `name:"nodeID" desc:"QueryNode ID to suspend"`
}

func (s *queryCoordState) SuspendNodeCommand(ctx context.Context, p *SuspendNodeParam) error {
	req := &querypb.SuspendNodeRequest{
		Base: &commonpb.MsgBase{
			TargetID: s.session.ServerID,
			SourceID: -1,
		},
		NodeID: p.NodeID,
	}

	resp, err := s.client.SuspendNode(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to suspend node %d: %w", p.NodeID, err)
	}
	if resp.ErrorCode != commonpb.ErrorCode_Success {
		return fmt.Errorf("suspend node %d failed: %s", p.NodeID, resp.Reason)
	}

	fmt.Printf("⏸️  Node %d suspended successfully\n", p.NodeID)
	return nil
}

type ResumeNodeParam struct {
	framework.ParamBase `use:"resume-node" desc:"Resume a QueryNode for resource operations"`
	NodeID              int64 `name:"nodeID" desc:"QueryNode ID to resume"`
}

func (s *queryCoordState) ResumeNodeCommand(ctx context.Context, p *ResumeNodeParam) error {
	req := &querypb.ResumeNodeRequest{
		Base: &commonpb.MsgBase{
			TargetID: s.session.ServerID,
			SourceID: -1,
		},
		NodeID: p.NodeID,
	}

	resp, err := s.client.ResumeNode(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to resume node %d: %w", p.NodeID, err)
	}
	if resp.ErrorCode != commonpb.ErrorCode_Success {
		return fmt.Errorf("resume node %d failed: %s", p.NodeID, resp.Reason)
	}

	fmt.Printf("✅ Node %d resumed successfully\n", p.NodeID)
	return nil
}

// Balance Management commands
type SuspendBalanceParam struct {
	framework.ParamBase `use:"suspend-balance" desc:"Suspend automatic load balancing"`
}

func (s *queryCoordState) SuspendBalanceCommand(ctx context.Context, p *SuspendBalanceParam) error {
	req := &querypb.SuspendBalanceRequest{
		Base: &commonpb.MsgBase{
			TargetID: s.session.ServerID,
			SourceID: -1,
		},
	}

	resp, err := s.client.SuspendBalance(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to suspend balance: %w", err)
	}
	if resp.ErrorCode != commonpb.ErrorCode_Success {
		return fmt.Errorf("suspend balance failed: %s", resp.Reason)
	}

	fmt.Printf("⏸️  Balance suspended successfully\n")
	return nil
}

type ResumeBalanceParam struct {
	framework.ParamBase `use:"resume-balance" desc:"Resume automatic load balancing"`
}

func (s *queryCoordState) ResumeBalanceCommand(ctx context.Context, p *ResumeBalanceParam) error {
	req := &querypb.ResumeBalanceRequest{
		Base: &commonpb.MsgBase{
			TargetID: s.session.ServerID,
			SourceID: -1,
		},
	}

	resp, err := s.client.ResumeBalance(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to resume balance: %w", err)
	}
	if resp.ErrorCode != commonpb.ErrorCode_Success {
		return fmt.Errorf("resume balance failed: %s", resp.Reason)
	}

	fmt.Printf("✅ Balance resumed successfully\n")
	return nil
}

type CheckBalanceStatusParam struct {
	framework.ParamBase `use:"check-balance-status" desc:"Check current balance status"`
}

func (s *queryCoordState) CheckBalanceStatusCommand(ctx context.Context, p *CheckBalanceStatusParam) error {
	req := &querypb.CheckBalanceStatusRequest{
		Base: &commonpb.MsgBase{
			TargetID: s.session.ServerID,
			SourceID: -1,
		},
	}

	resp, err := s.client.CheckBalanceStatus(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to check balance status: %w", err)
	}
	if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
		return fmt.Errorf("check balance status failed: %s", resp.Status.Reason)
	}

	status := "Suspended ⏸️"
	if resp.IsActive {
		status = "Active ✅"
	}
	fmt.Printf("Balance Status: %s\n", status)
	return nil
}

// Data Transfer commands
type TransferSegmentParam struct {
	framework.ParamBase `use:"transfer-segment" desc:"Transfer segment(s) between QueryNodes"`
	SourceNodeID        int64 `name:"sourceNode" desc:"Source QueryNode ID"`
	TargetNodeID        int64 `name:"targetNode" desc:"Target QueryNode ID (0 for all nodes)"`
	SegmentID           int64 `name:"segmentID" desc:"Specific segment ID (0 for all segments)"`
	TransferAll         bool  `name:"transferAll" default:"false" desc:"Transfer all segments from source node"`
	ToAllNodes          bool  `name:"toAllNodes" default:"false" desc:"Transfer to all available nodes"`
	CopyMode            bool  `name:"copyMode" default:"false" desc:"Copy mode (keep original)"`
}

func (s *queryCoordState) TransferSegmentCommand(ctx context.Context, p *TransferSegmentParam) error {
	req := &querypb.TransferSegmentRequest{
		Base: &commonpb.MsgBase{
			TargetID: s.session.ServerID,
			SourceID: -1,
		},
		SourceNodeID: p.SourceNodeID,
		TargetNodeID: p.TargetNodeID,
		SegmentID:    p.SegmentID,
		TransferAll:  p.TransferAll,
		ToAllNodes:   p.ToAllNodes,
		CopyMode:     p.CopyMode,
	}

	resp, err := s.client.TransferSegment(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to transfer segment: %w", err)
	}
	if resp.ErrorCode != commonpb.ErrorCode_Success {
		return fmt.Errorf("transfer segment failed: %s", resp.Reason)
	}

	fmt.Printf("✅ Segment transfer initiated successfully\n")
	fmt.Printf("   Source Node: %d\n", p.SourceNodeID)
	if p.ToAllNodes {
		fmt.Printf("   Target: All available nodes\n")
	} else if p.TargetNodeID > 0 {
		fmt.Printf("   Target Node: %d\n", p.TargetNodeID)
	}
	if p.TransferAll {
		fmt.Printf("   Segments: All segments\n")
	} else if p.SegmentID > 0 {
		fmt.Printf("   Segment: %d\n", p.SegmentID)
	}
	return nil
}

type TransferChannelParam struct {
	framework.ParamBase `use:"transfer-channel" desc:"Transfer channel(s) between QueryNodes"`
	SourceNodeID        int64  `name:"sourceNode" desc:"Source QueryNode ID"`
	TargetNodeID        int64  `name:"targetNode" desc:"Target QueryNode ID (0 for all nodes)"`
	ChannelName         string `name:"channelName" desc:"Specific channel name (empty for all channels)"`
	TransferAll         bool   `name:"transferAll" default:"false" desc:"Transfer all channels from source node"`
	ToAllNodes          bool   `name:"toAllNodes" default:"false" desc:"Transfer to all available nodes"`
	CopyMode            bool   `name:"copyMode" default:"false" desc:"Copy mode (keep original)"`
}

func (s *queryCoordState) TransferChannelCommand(ctx context.Context, p *TransferChannelParam) error {
	req := &querypb.TransferChannelRequest{
		Base: &commonpb.MsgBase{
			TargetID: s.session.ServerID,
			SourceID: -1,
		},
		SourceNodeID: p.SourceNodeID,
		TargetNodeID: p.TargetNodeID,
		ChannelName:  p.ChannelName,
		TransferAll:  p.TransferAll,
		ToAllNodes:   p.ToAllNodes,
		CopyMode:     p.CopyMode,
	}

	resp, err := s.client.TransferChannel(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to transfer channel: %w", err)
	}
	if resp.ErrorCode != commonpb.ErrorCode_Success {
		return fmt.Errorf("transfer channel failed: %s", resp.Reason)
	}

	fmt.Printf("✅ Channel transfer initiated successfully\n")
	fmt.Printf("   Source Node: %d\n", p.SourceNodeID)
	if p.ToAllNodes {
		fmt.Printf("   Target: All available nodes\n")
	} else if p.TargetNodeID > 0 {
		fmt.Printf("   Target Node: %d\n", p.TargetNodeID)
	}
	if p.TransferAll {
		fmt.Printf("   Channels: All channels\n")
	} else if p.ChannelName != "" {
		fmt.Printf("   Channel: %s\n", p.ChannelName)
	}
	return nil
}

type ReleaseCollectionParam struct {
	framework.ParamBase `use:"release collection" desc:"release collection"`
	CollectionID        int64 `name:"collection" desc:"collection id to release"`
}

func (s *queryCoordState) ReleaseCollectionCommand(ctx context.Context, p *ReleaseCollectionParam) error {
	req := &querypb.ReleaseCollectionRequest{
		Base: &commonpb.MsgBase{
			TargetID: s.session.ServerID,
			SourceID: -1,
		},
		CollectionID: p.CollectionID,
	}

	resp, err := s.client.ReleaseCollection(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to release collection: %w", err)
	}
	if resp.ErrorCode != commonpb.ErrorCode_Success {
		return fmt.Errorf("release collection failed: %s", resp.Reason)
	}

	fmt.Printf("✅ All collections released successfully\n")
	return nil
}

package mgrpc

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type dataCoordState struct {
	*framework.CmdState
	session   *models.Session
	client    datapb.DataCoordClient
	conn      *grpc.ClientConn
	prevState framework.State
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *dataCoordState) SetupCommands() {
	// cmd := &cobra.Command{}
	cmd := s.GetCmd()
	// cmd.AddCommand(
	// // metrics
	// getMetricsCmd(s.client),
	// // configuration
	// getConfigurationCmd(s.client, s.session.ServerID),

	// // compact
	// compactCmd(s.client),
	// )

	s.UpdateState(cmd, s, s.SetupCommands)
}

func GetDataCoordState(client datapb.DataCoordClient, conn *grpc.ClientConn, prev *framework.CmdState, session *models.Session) framework.State {
	state := &dataCoordState{
		CmdState:  prev.Spawn(fmt.Sprintf("DataCoord-%d(%s)", session.ServerID, session.Address)),
		session:   session,
		client:    client,
		conn:      conn,
		prevState: prev,
	}

	state.SetupCommands()

	return state
}

type CompactParam struct {
	framework.ParamBase `use:"compact" desc:"manual compact with collectionID"`
	CollectionID        int64   `name:"collectionID" default:"0" desc:"collection id to compact"`
	CompactAll          bool    `name:"compactAll" default:"false" desc:"explicitly allow compact all collections"`
	ChannelName         string  `name:"channelName" default:"" desc:"channel name to compact"`
	SegmentIDs          []int64 `name:"segmentID" default:"" desc:"segment ids to compact"`
}

func (s *dataCoordState) CompactCommand(ctx context.Context, p *CompactParam) error {
	if p.CollectionID <= 0 && !p.CompactAll {
		return errors.New("collection id not provided, set `compactAll` flag if needed")
	}
	resp, err := s.client.ManualCompaction(ctx, &milvuspb.ManualCompactionRequest{
		CollectionID: p.CollectionID,
		Channel:      p.ChannelName,
		SegmentIds:   p.SegmentIDs,
	})
	if err != nil {
		return errors.Wrapf(err, "manual compact fail with collectionID:%d", p.CollectionID)
	}
	fmt.Printf("manual compact done, collectionID:%d, compactionID:%d, rpc status:%v\n",
		p.CollectionID, resp.GetCompactionID(), resp.GetStatus())
	return nil
}

type FlushParam struct {
	framework.ParamBase `use:"flush" desc:"manual flush with collectionID"`
	CollectionID        int64 `name:"collectionID" default:"0" desc:"collection id to compact"`
}

func (s *dataCoordState) FlushCommand(ctx context.Context, p *FlushParam) error {
	resp, err := s.client.Flush(ctx, &datapb.FlushRequest{
		CollectionID: p.CollectionID,
	})
	if err != nil {
		return errors.Wrapf(err, "manual compact fail with collectionID:%d", p.CollectionID)
	}
	fmt.Printf("manual flush done, collectionID:%d, rpc status:%v\n",
		p.CollectionID, resp.GetStatus())
	return nil
}

type DCRecoveryInfoParam struct {
	framework.ParamBase `use:"recovery-info" desc:"get recovery info from datacoord for specified collectionID"`
	CollectionID        int64 `name:"collectionID" default:"0" desc:"collection id to compact"`
}

func (s *dataCoordState) GetRecoveryInfoCommand(ctx context.Context, p *DCRecoveryInfoParam) error {
	resp, err := s.client.GetRecoveryInfoV2(ctx, &datapb.GetRecoveryInfoRequestV2{
		CollectionID: p.CollectionID,
	})
	if err != nil {
		return errors.Wrapf(err, "manual compact fail with collectionID:%d", p.CollectionID)
	}
	// fmt.Printf("manual flush done, collectionID:%d, rpc status:%v\n",
	// 	p.CollectionID, resp.GetStatus())
	fmt.Println(resp.String())
	for _, info := range resp.GetChannels() {
		fmt.Println(info.ChannelName, info.FlushedSegmentIds, info.UnflushedSegmentIds)
	}
	return nil
}

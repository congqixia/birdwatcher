package mgrpc

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
)

type rootCoordState struct {
	*framework.CmdState
	session   *models.Session
	client    rootcoordpb.RootCoordClient
	conn      *grpc.ClientConn
	prevState framework.State
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *rootCoordState) SetupCommands() {
	cmd := s.GetCmd()
	s.UpdateState(cmd, s, s.SetupCommands)
}

type TestParam struct {
	framework.ParamBase `use:"pr"`
}

func (s *rootCoordState) TestCommand(ctx context.Context, p *TestParam) error {
	fmt.Println("rootcoord test")
	return nil
}

type DescribeCollectionParam struct {
	framework.ParamBase `use:"describe collection" desc:"describe collection by collectionID"`
	CollectionID        int64 `name:"collectionID" default:"0" desc:"collection id to compact"`
}

func (s *rootCoordState) DescribeCollectionCommand(ctx context.Context, p *DescribeCollectionParam) error {
	resp, err := s.client.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_DescribeCollection,
		},
		CollectionID: p.CollectionID,
	})
	if err != nil {
		return errors.Wrapf(err, "Describe collection with collectionID:%d", p.CollectionID)
	}
	fmt.Printf("CollectionID: %d, Name: %s, Schema: %s\n", resp.CollectionID, resp.CollectionName, resp.Schema.String())
	return nil
}

type AlterCollectionParam struct {
	framework.ParamBase `use:"alter collection" desc:"alter collection by collectionID"`
	DbName              string `name:"dbName" default:"" desc:"database name to alter"`
	CollectionID        int64  `name:"collectionID" default:"0" desc:"collection id to alter"`
	CollectionName      string `name:"collectionName" default:"" desc:"collection name to alter"`
	Key                 string `name:"key" default:""`
	Value               string `name:"value" default:""`
}

func (s *rootCoordState) AlterCollectionCommand(ctx context.Context, p *AlterCollectionParam) error {
	resp, err := s.client.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_AlterCollection,
		},
		DbName:         p.DbName,
		CollectionID:   p.CollectionID,
		CollectionName: p.CollectionName,
		Properties: []*commonpb.KeyValuePair{
			{
				Key:   p.Key,
				Value: p.Value,
			},
		},
	})
	if err != nil {
		return errors.Newf("alter collection fail: %s", err.Error())
	}
	fmt.Printf("AlterCollection resp: %v\n", resp)
	return nil
}

type DeleteCollectionPropertyParam struct {
	framework.ParamBase `use:"delete collection-property" desc:"alter collection by collectionID"`
	DbName              string `name:"dbName" default:"" desc:"database name to alter"`
	CollectionID        int64  `name:"collectionID" default:"0" desc:"collection id to alter"`
	CollectionName      string `name:"collectionName" default:"" desc:"collection name to alter"`
	Key                 string `name:"key" default:""`
	// Value               string `name:"value" default:""`
}

func (s *rootCoordState) DeleteCollectionPropertyCommand(ctx context.Context, p *DeleteCollectionPropertyParam) error {
	resp, err := s.client.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_AlterCollection,
		},
		DbName:         p.DbName,
		CollectionID:   p.CollectionID,
		CollectionName: p.CollectionName,
		DeleteKeys:     []string{p.Key},
	})
	if err != nil {
		return errors.Newf("alter collection fail: %s", err.Error())
	}
	fmt.Printf("AlterCollection resp: %v\n", resp)
	return nil
}

func GetRootCoordState(client rootcoordpb.RootCoordClient, conn *grpc.ClientConn, prev *framework.CmdState, session *models.Session) framework.State {
	state := &rootCoordState{
		session:   session,
		CmdState:  prev.Spawn(fmt.Sprintf("RootCoord-%d(%s)", session.ServerID, session.Address)),
		client:    client,
		conn:      conn,
		prevState: prev,
	}

	state.SetupCommands()

	return state
}

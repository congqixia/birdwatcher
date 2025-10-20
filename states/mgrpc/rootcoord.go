package mgrpc

import (
	"context"
	"fmt"

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

type AlterFieldParam struct {
	framework.ParamBase `use:"alter-field" desc:"alter collection field"`
	CollectionName      string `name:"collectionName" default:"" desc:"collection name to balance"`
	DbName              string `name:"dbName" default:""`
	FieldName           string `name:"fieldName" default:""`
	Key                 string `name:"key" default:""`
	Value               string `name:"value" default:""`
}

func (s *rootCoordState) AlterFieldCommand(ctx context.Context, p *AlterFieldParam) error {
	resp, err := s.client.AlterCollectionField(ctx, &milvuspb.AlterCollectionFieldRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_AlterCollectionField,
			TargetID: s.session.ServerID,
		},
		DbName:         p.DbName,
		CollectionName: p.CollectionName,
		FieldName:      p.FieldName,
		Properties: []*commonpb.KeyValuePair{
			{Key: p.Key, Value: p.Value},
		},
	})
	if err != nil {
		return err
	}
	fmt.Println(resp)
	return nil
}

type RemoveCollectionAttributeParam struct {
	framework.ParamBase `use:"remove collection-attr" desc:"remove collection attribute"`
	DbName              string   `name:"dbName" default:""`
	CollectionName      string   `name:"collectionName" default:""`
	Keys                []string `name:"keys"`
}

func (s *rootCoordState) RemoveCollectionAttributeCommand(ctx context.Context, p *RemoveCollectionAttributeParam) error {
	resp, err := s.client.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_AlterCollection,
			TargetID: s.session.ServerID,
		},
		DbName:         p.DbName,
		CollectionName: p.CollectionName,
		DeleteKeys:     p.Keys,
	})
	if err != nil {
		return err
	}
	fmt.Println(resp)
	return nil
}

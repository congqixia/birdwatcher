package states

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	commonpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/commonpb"
	datapbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/datapb"
	"github.com/milvus-io/birdwatcher/proto/v2.2/indexpb"
	"github.com/milvus-io/birdwatcher/proto/v2.2/milvuspb"
	querypbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/querypb"
	rootcoordpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/rootcoordpb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/samber/lo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type LoadCollectionParam struct {
	framework.ParamBase `use:"load collection" desc:"load collection"`
	DB                  int64 `name:"db" default:"0" desc:"db id"`
	Collection          int64 `name:"collection" default:"0" desc:"collection ID"`
	ReplicaNum          int64 `name:"replica" default:"1" desc:"collection"`
}

func (s *InstanceState) LoadCollectionCommand(ctx context.Context, p *LoadCollectionParam) error {
	sessions, err := common.ListSessions(s.client, s.basePath)
	if err != nil {
		return err
	}
	rcSession, found := lo.Find(sessions, func(session *models.Session) bool {
		return session.ServerName == "rootcoord"
	})

	if !found {
		return errors.New("rootcoord session not found")
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	}

	conn, err := grpc.DialContext(ctx, rcSession.Address, opts...)
	if err != nil {
		return errors.New("find to connect to rootcoord")
	}
	rootcoord := rootcoordpbv2.NewRootCoordClient(conn)

	resp, err := rootcoord.DescribeCollectionInternal(ctx, &milvuspb.DescribeCollectionRequest{
		Base: &commonpbv2.MsgBase{
			TargetID: rcSession.ServerID,
			MsgType:  commonpbv2.MsgType_DescribeCollection,
		},
		CollectionID: p.Collection,
	})
	if err != nil {
		return err
	}

	schema := resp.GetSchema()

	dcSession, found := lo.Find(sessions, func(session *models.Session) bool {
		return session.ServerName == "datacoord"
	})
	if !found {
		return errors.New("rootcoord session not found")
	}
	conn, err = grpc.DialContext(ctx, dcSession.Address, opts...)
	if err != nil {
		return errors.New("find to connect to rootcoord")
	}
	datacoord := datapbv2.NewDataCoordClient(conn)

	idxResp, err := datacoord.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{
		CollectionID: p.Collection,
	})
	if err != nil {
		return err
	}

	fieldIndexIDs := make(map[int64]int64)
	for _, index := range idxResp.IndexInfos {
		fieldIndexIDs[index.FieldID] = index.IndexID
	}

	qcSession, found := lo.Find(sessions, func(session *models.Session) bool {
		return session.ServerName == "querycoord"
	})
	if !found {
		return errors.New("querycoord session not found")
	}
	conn, err = grpc.DialContext(ctx, qcSession.Address, opts...)
	if err != nil {
		return errors.New("find to connect to querycoord")
	}
	querycoord := querypbv2.NewQueryCoordClient(conn)

	request := &querypbv2.LoadCollectionRequest{
		Base: &commonpbv2.MsgBase{
			TargetID: qcSession.ServerID,
			MsgType:  commonpbv2.MsgType_LoadCollection,
		},
		DbID:           0,
		CollectionID:   p.Collection,
		Schema:         schema,
		ReplicaNumber:  int32(p.ReplicaNum),
		FieldIndexID:   fieldIndexIDs,
		Refresh:        false,
		ResourceGroups: nil,
	}
	loadCollectionResp, err := querycoord.LoadCollection(ctx, request)
	if err != nil {
		return err
	}

	fmt.Printf("load collection: %s\n", loadCollectionResp.GetErrorCode().String())

	return nil
}

type ReleaseCollectionParam struct {
	framework.ParamBase `use:"release collection" desc:"release collection"`
	DB                  int64 `name:"db" default:"0" desc:"db id"`
	Collection          int64 `name:"collection" default:"0" desc:"collection ID"`
}

func (s *InstanceState) ReleaseCollectionCommand(ctx context.Context, p *ReleaseCollectionParam) error {
	sessions, err := common.ListSessions(s.client, s.basePath)
	if err != nil {
		return err
	}
	qcSession, found := lo.Find(sessions, func(session *models.Session) bool {
		return session.ServerName == "querycoord"
	})
	if !found {
		return errors.New("querycoord session not found")
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	}
	conn, err := grpc.DialContext(ctx, qcSession.Address, opts...)
	if err != nil {
		return errors.New("find to connect to querycoord")
	}
	querycoord := querypbv2.NewQueryCoordClient(conn)

	req := &querypbv2.ReleaseCollectionRequest{
		Base: &commonpbv2.MsgBase{
			TargetID: qcSession.ServerID,
		},
		CollectionID: p.Collection,
	}

	resp, err := querycoord.ReleaseCollection(ctx, req)
	if err != nil {
		return err
	}

	fmt.Printf("load collection: %s\n", resp.GetErrorCode().String())
	return nil
}

type CompactCollectionParam struct {
	framework.ParamBase `use:"compact collection" desc:"release collection"`
	DB                  int64 `name:"db" default:"0" desc:"db id"`
	Collection          int64 `name:"collection" default:"0" desc:"collection ID"`
	Major               bool  `name:"major" default:"false" desc:"is major"`
}

func (s *InstanceState) CompactCollectionCommand(ctx context.Context, p *CompactCollectionParam) error {
	sessions, err := common.ListSessions(s.client, s.basePath)
	if err != nil {
		return err
	}
	dcSession, found := lo.Find(sessions, func(session *models.Session) bool {
		return session.ServerName == "datacoord"
	})
	if !found {
		return errors.New("querycoord session not found")
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	}
	conn, err := grpc.DialContext(ctx, dcSession.Address, opts...)
	if err != nil {
		return errors.New("find to connect to querycoord")
	}
	datacoord := datapbv2.NewDataCoordClient(conn)

	resp, err := datacoord.ManualCompaction(ctx, &milvuspb.ManualCompactionRequest{
		CollectionID:    p.Collection,
		MajorCompaction: p.Major,
	})
	if err != nil {
		return err
	}

	fmt.Printf("compact collection: %s, planID: %d\n", resp.GetStatus().GetErrorCode().String(), resp.GetCompactionID())
	return nil
}

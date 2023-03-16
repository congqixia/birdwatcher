package states

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.2/commonpb"
	commonpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/commonpb"
	indexpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/indexpb"
	"github.com/milvus-io/birdwatcher/proto/v2.2/internalpb"
	"github.com/milvus-io/birdwatcher/proto/v2.2/planpb"
	querypbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/querypb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

func GetProbeCmd(cli clientv3.KV, basePath string) *cobra.Command {
	probeCmd := &cobra.Command{
		Use:   "probe",
		Short: "probe service state with internal apis",
	}

	probeCmd.AddCommand(
		// probe query
		getProbeQueryCmd(cli, basePath),
	)

	return probeCmd
}

func getProbeQueryCmd(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query",
		Short: "probe query service",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			probeQuery(ctx, cli, basePath, func() {})
		},
	}

	cmd.Flags().Int64("collection", 0, "collection id to probe")
	cmd.Flags().Int64("segment", 0, "segment id to probe")
	return cmd
}

func probeQuery(ctx context.Context, cli clientv3.KV, basePath string, afterFail func()) {
	loaded, err := common.ListCollectionLoadedInfo(ctx, cli, basePath, models.GTEVersion2_2)
	if err != nil {
		fmt.Println("failed to list loaded collection", err.Error())
		return
	}

	if len(loaded) == 0 {
		fmt.Println("no loaded collection")
		return
	}

	sessions, err := common.ListSessions(cli, basePath)
	if err != nil {
		fmt.Println("failed to list online sessions", err.Error())
		return
	}

	qc, err := getQueryCoordClient(sessions)
	if err != nil {
		fmt.Println("failed to connect querycoord", err.Error())
		return
	}

	qns, err := getQueryNodeClients(sessions)
	if err != nil {
		fmt.Println("failed to connect querynodes", err.Error())
		return
	}
	if len(qns) == 0 {
		fmt.Println("no querynode online")
		return
	}

	for _, collection := range loaded {
		fmt.Println("probing collection", collection.CollectionID)
		req, err := getMockSearchRequest(ctx, cli, basePath, collection)
		if err != nil {
			fmt.Println("failed to generated mock request", err.Error())
			continue
		}

		leaders, err := qc.GetShardLeaders(ctx, &querypbv2.GetShardLeadersRequest{
			Base:         &commonpbv2.MsgBase{},
			CollectionID: collection.CollectionID,
		})
		if err != nil {
			fmt.Println("querycoord get shard leaders error", err.Error())
			continue
		}

		for _, shard := range leaders.GetShards() {

			for _, nodeID := range shard.GetNodeIds() {
				qn, ok := qns[nodeID]
				if !ok {
					fmt.Printf("Shard leader %d not online\n", nodeID)
					continue
				}

				ctx, cancel := context.WithTimeout(ctx, time.Second*5)
				req.DmlChannels = []string{shard.GetChannelName()}
				req.Req.Base.TargetID = nodeID
				resp, err := qn.Search(ctx, req)
				cancel()
				if err != nil {
					fmt.Printf("Shard %s Leader[%d] failed to search with eventually consistency level, err: %s\n", shard.GetChannelName(), nodeID, err.Error())
					afterFail()
					continue
				}
				if resp.GetStatus().GetErrorCode() != commonpbv2.ErrorCode_Success {
					fmt.Printf("Shard %s Leader[%d] failed to search,error code: %s reason:%s\n", shard.GetChannelName(), nodeID, resp.GetStatus().GetErrorCode().String(), resp.GetStatus().GetReason())
					afterFail()
					continue
				}
				fmt.Printf("Shard %s leader[%d] probe with search success.\n", shard.GetChannelName(), nodeID)
			}
		}

	}
}

func getQueryCoordClient(sessions []*models.Session) (querypbv2.QueryCoordClient, error) {
	for _, session := range sessions {
		if strings.ToLower(session.ServerName) != "querycoord" {
			continue
		}

		opts := []grpc.DialOption{
			grpc.WithInsecure(),
			grpc.WithBlock(),
			grpc.WithTimeout(2 * time.Second),
		}

		conn, err := grpc.DialContext(context.Background(), session.Address, opts...)
		if err != nil {
			fmt.Printf("failed to connect %s(%d), err: %s\n", session.ServerName, session.ServerID, err.Error())
			continue
		}

		client := querypbv2.NewQueryCoordClient(conn)
		return client, nil

	}
	return nil, errors.New("querycoord session not found")
}

func getQueryNodeClients(sessions []*models.Session) (map[int64]querypbv2.QueryNodeClient, error) {

	result := make(map[int64]querypbv2.QueryNodeClient)

	for _, session := range sessions {
		if strings.ToLower(session.ServerName) != "querynode" {
			continue
		}
		opts := []grpc.DialOption{
			grpc.WithInsecure(),
			grpc.WithBlock(),
			grpc.WithTimeout(2 * time.Second),
		}

		conn, err := grpc.DialContext(context.Background(), session.Address, opts...)
		if err != nil {
			fmt.Printf("failed to connect %s(%d), err: %s\n", session.ServerName, session.ServerID, err.Error())
			continue
		}

		client := querypbv2.NewQueryNodeClient(conn)
		result[session.ServerID] = client
	}

	return result, nil
}

func getMockSearchRequest(ctx context.Context, cli clientv3.KV, basePath string, collection *models.CollectionLoaded) (*querypbv2.SearchRequest, error) {
	coll, err := common.GetCollectionByIDVersion(ctx, cli, basePath, models.GTEVersion2_2, collection.CollectionID)
	if err != nil {
		return nil, err
	}
	pkField, ok := coll.GetPKField()
	if !ok {
		return nil, errors.New("pk field not found")
	}
	vectorField, ok := coll.GetVectorField()
	if !ok {
		return nil, errors.New("vector field not found")
	}
	dim, err := vectorField.GetDim()
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	indexID := collection.FieldIndexID[vectorField.FieldID]
	fmt.Printf("Found vector field %s(%d) with dim[%d], indexID: %d\n", vectorField.Name, vectorField.FieldID, dim, indexID)

	indexes, _, err := common.ListProtoObjects(ctx, cli, path.Join(basePath, "field-index"), func(index *indexpbv2.FieldIndex) bool {
		return index.GetIndexInfo().GetIndexID() == indexID
	})

	if err != nil {
		return nil, err
	}

	if len(indexes) != 1 {
		fmt.Println("multiple or zero index found, bad meta")
		return nil, err
	}
	vector := genFloatVector(dim)

	req := &internalpb.SearchRequest{
		Base: &commonpbv2.MsgBase{
			MsgType: commonpb.MsgType_Search,
		},
		CollectionID:       coll.ID,
		PartitionIDs:       []int64{},
		Dsl:                "",
		PlaceholderGroup:   vector2PlaceholderGroupBytes(vector),
		DslType:            commonpbv2.DslType_BoolExprV1,
		GuaranteeTimestamp: 1, //Eventually first
		TravelTimestamp:    math.MaxUint64,
		Nq:                 1,
	}

	index := indexes[0]

	indexType := common.GetKVPair(index.GetIndexInfo().GetIndexParams(), "index_type")

	switch indexType {
	case "HNSW":
		raw := common.GetKVPair(index.GetIndexInfo().GetIndexParams(), "efConstruction")
		efConstruction, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			efConstruction = 360 // in case of auto index
		}
		metricType := common.GetKVPair(index.GetIndexInfo().GetIndexParams(), "metric_type")
		topK := rand.Int63n(efConstruction-1) + 1

		/*
			searchParams := map[string]string{
				"anns_field":    vectorField.Name,
				"topk":          fmt.Sprintf("%d", topK),
				"params":        string(genSearchHNSWParamBytes(topK)),
				"metric_type":   metricType,
				"round_decimal": "-1",
			}
			bs, err := json.Marshal(searchParams)
			spStr := string(bs)
			fmt.Println("search params", spStr)*/
		spStr := genSearchHNSWParamBytes(topK)

		req.SerializedExprPlan = getSearchPlan(vectorField.DataType == models.DataTypeBinaryVector, pkField.FieldID, vectorField.FieldID, topK, metricType, string(spStr))

		r := &querypbv2.SearchRequest{
			Req:             req,
			FromShardLeader: false,
			DmlChannels:     []string{},
		}
		return r, nil

	default:
		return nil, fmt.Errorf("probing index type %s not supported yet", indexType)
	}

}

func getSearchPlan(isBinary bool, pkFieldID, vectorFieldID int64, topk int64, metricType string, searchParam string) []byte {
	plan := &planpb.PlanNode{
		Node: &planpb.PlanNode_VectorAnns{
			VectorAnns: &planpb.VectorANNS{
				IsBinary:   isBinary,
				Predicates: nil, // empty
				QueryInfo: &planpb.QueryInfo{
					Topk:         topk,
					MetricType:   metricType,
					SearchParams: searchParam,
					RoundDecimal: -1,
				},
				PlaceholderTag: "$0",
				FieldId:        vectorFieldID,
			},
		},
		OutputFieldIds: []int64{pkFieldID},
	}

	bs, _ := proto.Marshal(plan)

	return bs
}

func genSearchHNSWParamBytes(ef int64) []byte {
	m := make(map[string]any)
	m["ef"] = ef
	bs, _ := json.Marshal(m)
	return bs
}

func genFloatVector(dim int64) FloatVector {
	result := make([]float32, 0, dim)

	for i := int64(0); i < dim; i++ {
		result = append(result, rand.Float32())
	}

	return FloatVector(result)
}

type FloatVector []float32

func (fv FloatVector) Serialize() []byte {
	data := make([]byte, 0, 4*len(fv)) // float32 occupies 4 bytes
	buf := make([]byte, 4)
	for _, f := range fv {
		binary.LittleEndian.PutUint32(buf, math.Float32bits(f))
		data = append(data, buf...)
	}
	return data
}

func (fv FloatVector) DataType() commonpbv2.PlaceholderType {
	return commonpbv2.PlaceholderType_FloatVector
}

func vector2PlaceholderGroupBytes[T interface {
	Serialize() []byte
	DataType() commonpbv2.PlaceholderType
}](vector T) []byte {
	phg := &commonpbv2.PlaceholderGroup{
		Placeholders: []*commonpbv2.PlaceholderValue{
			vector2Placeholder(vector),
		},
	}

	bs, _ := proto.Marshal(phg)
	return bs
}

func vector2Placeholder[T interface {
	Serialize() []byte
	DataType() commonpbv2.PlaceholderType
}](vector T) *commonpbv2.PlaceholderValue {
	ph := &commonpbv2.PlaceholderValue{
		Tag: "$0",
	}

	ph.Type = vector.DataType()
	ph.Values = append(ph.Values, vector.Serialize())
	return ph
}

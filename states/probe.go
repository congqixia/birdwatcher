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

	"github.com/samber/lo"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

func GetProbeCmd(cli kv.MetaKV, basePath string) *cobra.Command {
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

func getProbeQueryCmd(cli kv.MetaKV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query",
		Short: "probe query service",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			loaded, err := common.ListCollectionLoadedInfo(ctx, cli, basePath)
			if err != nil {
				fmt.Println("failed to list loaded collection", err.Error())
				return
			}

			if len(loaded) == 0 {
				fmt.Println("no loaded collection")
				return
			}

			sessions, err := common.ListSessions(ctx, cli, basePath)
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

			for _, info := range loaded {
				collection := info.GetProto()
				fmt.Println("probing collection", collection.CollectionID)
				req, err := getMockSearchRequest(ctx, cli, basePath, info)
				if err != nil {
					fmt.Println("failed to generated mock request", err.Error())
					continue
				}

				leaders, err := qc.GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{
					Base:         &commonpb.MsgBase{},
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
							continue
						}
						if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
							fmt.Printf("Shard %s Leader[%d] failed to search,error code: %s reason:%s\n", shard.GetChannelName(), nodeID, resp.GetStatus().GetErrorCode().String(), resp.GetStatus().GetReason())
							continue
						}
						fmt.Printf("Shard %s leader[%d] probe with search success.\n", shard.GetChannelName(), nodeID)
					}
				}
			}
		},
	}

	return cmd
}

type ProbePKParam struct {
	framework.ParamBase `use:"probe pk" desc:"probe pk in segment"`
	CollectionID        int64    `name:"collection" default:"0" desc:"collection id to probe"`
	PK                  string   `name:"pk" default:"" desc:"pk value to probe"`
	OutputFields        []string `name:"outputField" default:"" desc:"output fields list"`
	MvccTimestamp       int64    `name:"mvccTimestamp" default:"0" desc:"mvcc timestamp to probe"`
}

func (s *InstanceState) ProbePKCommand(ctx context.Context, p *ProbePKParam) error {
	coll, err := common.GetCollectionByIDVersion(ctx, s.client, s.basePath, p.CollectionID)
	if err != nil {
		return err
	}

	pkf, _ := coll.GetPKField()
	var datatype schemapb.DataType
	var val *planpb.GenericValue
	switch pkf.DataType {
	case models.DataTypeVarChar:
		datatype = schemapb.DataType_VarChar
		val = &planpb.GenericValue{
			Val: &planpb.GenericValue_StringVal{
				StringVal: p.PK,
			},
		}
	case models.DataTypeInt64:
		datatype = schemapb.DataType_Int64
		pkv, err := strconv.ParseInt(p.PK, 10, 64)
		if err != nil {
			return err
		}
		val = &planpb.GenericValue{
			Val: &planpb.GenericValue_Int64Val{
				Int64Val: pkv,
			},
		}
	}

	var outputFields []int64
	fieldIDName := make(map[int64]string)
	outputSet := lo.SliceToMap(p.OutputFields, func(output string) (string, struct{}) {
		return output, struct{}{}
	})
	for _, f := range coll.GetProto().Schema.Fields {
		_, has := outputSet[f.Name]
		if (f.FieldID >= 100 && has) || f.IsPrimaryKey {
			outputFields = append(outputFields, f.FieldID)
		}
		fieldIDName[f.FieldID] = f.Name
	}
	if _, has := outputSet["$ts"]; has {
		outputFields = append(outputFields, 1)
	}
	plan := &planpb.PlanNode{
		Node: &planpb.PlanNode_Predicates{
			Predicates: &planpb.Expr{
				Expr: &planpb.Expr_TermExpr{
					TermExpr: &planpb.TermExpr{
						ColumnInfo: &planpb.ColumnInfo{
							FieldId:      pkf.FieldID,
							DataType:     datatype,
							IsAutoID:     pkf.AutoID,
							IsPrimaryKey: pkf.IsPrimaryKey,
						},
						Values: []*planpb.GenericValue{
							val,
						},
						IsInField: true,
					},
				},
			},
		},
		OutputFieldIds: outputFields,
	}

	bs, _ := proto.Marshal(plan)

	sessions, err := common.ListSessions(ctx, s.client, s.basePath)
	if err != nil {
		fmt.Println("failed to list online sessions", err.Error())
		return err
	}

	qc, err := getQueryCoordClient(sessions)
	if err != nil {
		fmt.Println("failed to connect querycoord", err.Error())
		return err
	}

	resp, err := qc.GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{
		Base:         &commonpb.MsgBase{},
		CollectionID: p.CollectionID,
	})
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		fmt.Println("failed to list shard leader", resp.GetStatus().GetErrorCode())
		return err
	}

	qns, err := getQueryNodeClients(sessions)
	if err != nil {
		fmt.Println("failed to connect querynodes", err.Error())
		return err
	}
	if len(qns) == 0 {
		fmt.Println("no querynode online")
		return nil
	}

	// use current ts when mvccTimestamp not specified
	if p.MvccTimestamp == 0 {
		p.MvccTimestamp = int64(ComposeTS(time.Now().UnixMilli(), 0))
	}

	for nodeID, qn := range qns {
		resp, err := qn.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{
			Base: &commonpb.MsgBase{TargetID: nodeID},
		})
		if err != nil {
			fmt.Println("failed to get data distribution from node", nodeID, err.Error())
			continue
		}
		for _, segInfo := range resp.GetSegments() {
			if segInfo.GetCollection() != p.CollectionID {
				continue
			}
			if segInfo.GetLevel() == datapb.SegmentLevel_L0 {
				continue
			}
			result, err := qn.QuerySegments(ctx, &querypb.QueryRequest{
				SegmentIDs:      []int64{segInfo.ID},
				DmlChannels:     []string{segInfo.GetChannel()},
				FromShardLeader: true, // query single segment
				Scope:           querypb.DataScope_Historical,
				Req: &internalpb.RetrieveRequest{
					Base:               &commonpb.MsgBase{TargetID: nodeID, MsgID: time.Now().Unix()},
					CollectionID:       segInfo.Collection,
					PartitionIDs:       []int64{segInfo.Partition},
					SerializedExprPlan: bs,
					OutputFieldsId:     outputFields,
					Limit:              -1, // unlimited
					MvccTimestamp:      uint64(p.MvccTimestamp),
				},
			})
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
			if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
				fmt.Printf("failed to probe pk on segment %d, reason: %s\n", segInfo.GetID(), result.GetStatus().GetReason())
				continue
			}

			if GetSizeOfIDs(result.GetIds()) == 0 {
				continue
			}
			fmt.Printf("PK %s found on segment %d\n", p.PK, segInfo.GetID())
			for _, fd := range result.GetFieldsData() {
				if spv := fd.GetVectors().GetSparseFloatVector(); spv != nil {
					fmt.Printf("Field %s(SparseFloatVector)]")
					for _, bs := range spv.GetContents() {
						result, err := deserializeInt32Float32Pairs(bs)
						if err != nil {
							fmt.Println("failed to deserde spv, error: ", err.Error())
						}
						fmt.Println("Parsed spv: ", result)
					}
				} else {
					fmt.Printf("Field %s, value: %v\n", fieldIDName[fd.GetFieldId()], fd.GetField())
				}
				fmt.Printf("Field %s, valid data: %v\n", fieldIDName[fd.GetFieldId()], fd.GetValidData())
			}
		}
	}

	return nil
}

func GetSizeOfIDs(data *schemapb.IDs) int {
	result := 0
	if data.IdField == nil {
		return result
	}

	switch data.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		result = len(data.GetIntId().GetData())
	case *schemapb.IDs_StrId:
		result = len(data.GetStrId().GetData())
	default:
	}

	return result
}

func getQueryCoordClient(sessions []*models.Session) (querypb.QueryCoordClient, error) {
	for _, session := range sessions {
		if strings.ToLower(session.ServerName) != "querycoord" && strings.ToLower(session.ServerName) != "mixcoord" {
			continue
		}

		opts := []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		}

		var conn *grpc.ClientConn
		var err error
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			conn, err = grpc.DialContext(ctx, session.Address, opts...)
		}()
		if err != nil {
			fmt.Printf("failed to connect %s(%d), err: %s\n", session.ServerName, session.ServerID, err.Error())
			continue
		}

		client := querypb.NewQueryCoordClient(conn)
		return client, nil
	}
	return nil, errors.New("querycoord session not found")
}

func getQueryNodeClients(sessions []*models.Session) (map[int64]querypb.QueryNodeClient, error) {
	result := make(map[int64]querypb.QueryNodeClient)

	for _, session := range sessions {
		if strings.ToLower(session.ServerName) != "querynode" {
			continue
		}
		opts := []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		}

		var conn *grpc.ClientConn
		var err error
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			conn, err = grpc.DialContext(ctx, session.Address, opts...)
		}()
		if err != nil {
			fmt.Printf("failed to connect %s(%d), err: %s\n", session.ServerName, session.ServerID, err.Error())
			continue
		}

		client := querypb.NewQueryNodeClient(conn)
		result[session.ServerID] = client
	}

	return result, nil
}

func getMockSearchRequest(ctx context.Context, cli kv.MetaKV, basePath string, info *models.CollectionLoaded) (*querypb.SearchRequest, error) {
	collection := info.GetProto()
	coll, err := common.GetCollectionByIDVersion(ctx, cli, basePath, collection.CollectionID)
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

	indexes, _, err := common.ListProtoObjects(ctx, cli, path.Join(basePath, "field-index"), func(index *indexpb.FieldIndex) bool {
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
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_Search,
		},
		CollectionID:       coll.GetProto().ID,
		PartitionIDs:       []int64{},
		Dsl:                "",
		PlaceholderGroup:   vector2PlaceholderGroupBytes(vector),
		DslType:            commonpb.DslType_BoolExprV1,
		GuaranteeTimestamp: 1, // Eventually first
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
		if metricType == "" {
			metricType = common.GetKVPair(index.GetIndexInfo().GetTypeParams(), "metric_type")
			if metricType == "" {
				fmt.Println("no metric_type in IndexParams or TypeParams")
				return nil, fmt.Errorf("no metric_type in IndexParams or TypeParams, bad meta")
			}
			fmt.Println("metric_type is in TypeParams instead of IndexParams")
		}
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

		r := &querypb.SearchRequest{
			Req:             req,
			FromShardLeader: false,
			DmlChannels:     []string{},
		}
		return r, nil
	case "DISKANN":
		metricType := common.GetKVPair(index.GetIndexInfo().GetIndexParams(), "metric_type")
		if metricType == "" {
			metricType = common.GetKVPair(index.GetIndexInfo().GetTypeParams(), "metric_type")
			if metricType == "" {
				fmt.Println("no metric_type in IndexParams or TypeParams")
				return nil, fmt.Errorf("no metric_type in IndexParams or TypeParams, bad meta")
			}
			fmt.Println("metric_type is in TypeParams instead of IndexParams")
		}

		topK := int64(10)
		spStr := genSearchDISKANNParamBytes(20)
		req.SerializedExprPlan = getSearchPlan(vectorField.DataType == models.DataTypeBinaryVector, pkField.FieldID, vectorField.FieldID, topK, metricType, string(spStr))
		r := &querypb.SearchRequest{
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
				// TODO
				VectorType: planpb.VectorType_Float16Vector,
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

func genSearchDISKANNParamBytes(searchList int) []byte {
	m := make(map[string]any)
	m["search_list"] = searchList
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

func (fv FloatVector) DataType() commonpb.PlaceholderType {
	return commonpb.PlaceholderType_FloatVector
}

func vector2PlaceholderGroupBytes[T interface {
	Serialize() []byte
	DataType() commonpb.PlaceholderType
}](vector T) []byte {
	phg := &commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{
			vector2Placeholder(vector),
		},
	}

	bs, _ := proto.Marshal(phg)
	return bs
}

type Int32Float32Pair struct {
	Index uint32
	Value float32
}

func deserializeInt32Float32Pairs(data []byte) ([]Int32Float32Pair, error) {
	const pairSize = 8 // 4 bytes int32 + 4 bytes float32
	if len(data)%pairSize != 0 {
		return nil, fmt.Errorf("data length %d is not a multiple of %d", len(data), pairSize)
	}
	result := make([]Int32Float32Pair, 0, len(data)/pairSize)
	for i := 0; i < len(data); i += pairSize {
		idx := binary.LittleEndian.Uint32(data[i : i+4])
		val := math.Float32frombits(binary.LittleEndian.Uint32(data[i+4 : i+8]))
		result = append(result, Int32Float32Pair{Index: idx, Value: val})
	}
	return result, nil
}

func vector2Placeholder[T interface {
	Serialize() []byte
	DataType() commonpb.PlaceholderType
}](vector T) *commonpb.PlaceholderValue {
	ph := &commonpb.PlaceholderValue{
		Tag: "$0",
	}

	ph.Type = vector.DataType()
	ph.Values = append(ph.Values, vector.Serialize())
	return ph
}

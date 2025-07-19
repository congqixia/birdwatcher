package states

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
)

type HealthzCheckParam struct {
	framework.ParamBase `use:"healthz-check" desc:"perform healthz check for connect instance"`
}

type HealthzCheckReports struct {
	framework.ListResultSet[*HealthzCheckReport]
}

func (rs *HealthzCheckReports) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		sb := &strings.Builder{}
		for _, report := range rs.Data {
			fmt.Fprintln(sb, report.Msg)
		}
		return sb.String()
	case framework.FormatJSON:
		sb := &strings.Builder{}
		for _, report := range rs.Data {
			output := report.Extra
			bs, err := json.Marshal(output)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
			sb.Write(bs)
			sb.WriteString("\n")
		}
		return sb.String()
	default:
	}
	return ""
}

type HealthzCheckReport struct {
	Msg   string
	Extra map[string]any
}

func (c *InstanceState) HealthzCheckCommand(ctx context.Context, p *HealthzCheckParam) (*framework.PresetResultSet, error) {
	results, err := c.checkSegmentTarget(ctx)
	if err != nil {
		return nil, err
	}

	collectionResults, err := c.checkCollectionMeta(ctx)
	if err != nil {
		return nil, err
	}
	results = append(results, collectionResults...)

	return framework.NewPresetResultSet(framework.NewListResult[HealthzCheckReports](results), framework.FormatJSON), nil
}

func (c *InstanceState) checkSegmentTarget(ctx context.Context) ([]*HealthzCheckReport, error) {
	segments, err := common.ListSegments(ctx, c.client, c.basePath)
	if err != nil {
		return nil, err
	}
	validIDs := lo.SliceToMap(segments, func(segment *models.Segment) (int64, struct{}) { return segment.ID, struct{}{} })

	sessions, err := common.ListSessions(ctx, c.client, c.basePath)
	if err != nil {
		return nil, err
	}

	var results []*HealthzCheckReport

	for _, session := range sessions {
		opts := []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		}

		conn, err := grpc.DialContext(ctx, session.Address, opts...)
		if err != nil {
			fmt.Printf("failed to connect %s(%d), err: %s\n", session.ServerName, session.ServerID, err.Error())
			continue
		}

		if session.ServerName == "querynode" {
			clientv2 := querypb.NewQueryNodeClient(conn)
			resp, err := clientv2.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{
				Base: &commonpb.MsgBase{
					SourceID: -1,
					TargetID: session.ServerID,
				},
			})
			if err != nil {
				fmt.Println(err.Error())
				continue
			}

			for _, segment := range resp.GetSegments() {
				if _, ok := validIDs[segment.GetID()]; !ok {
					results = append(results, &HealthzCheckReport{
						Msg: fmt.Sprintf("Sealed segment %d still loaded while meta gc-ed", segment.GetID()),
						Extra: map[string]any{
							"segment_id":    segment.GetID(),
							"segment_state": "sealed",
						},
					})
				}
			}

			for _, lv := range resp.GetLeaderViews() {
				growings := lo.Uniq(lo.Union(lv.GetGrowingSegmentIDs(), lo.Keys(lv.GetGrowingSegments())))
				for _, segmentID := range growings {
					if _, ok := validIDs[segmentID]; !ok {
						results = append(results, &HealthzCheckReport{
							Msg: fmt.Sprintf("Sealed segment %d still loaded while meta gc-ed", segmentID),
							Extra: map[string]any{
								"segment_id":    segmentID,
								"segment_state": "growing",
							},
						})
					}
				}
			}
		}
	}
	return results, nil
}

func (c *InstanceState) checkCollectionMeta(ctx context.Context) ([]*HealthzCheckReport, error) {
	collections, err := common.ListCollections(ctx, c.client, c.basePath)
	if err != nil {
		return nil, err
	}

	var results []*HealthzCheckReport
	sessions, err := common.ListSessions(ctx, c.client, c.basePath)
	if err != nil {
		return nil, err
	}

	inMeta := lo.SliceToMap(collections, func(collection *models.Collection) (int64, struct{}) {
		return collection.GetProto().GetID(), struct{}{}
	})

	dbs, err := common.ListDatabase(ctx, c.client, c.basePath)
	if err != nil {
		fmt.Println("failed to list database info", err.Error())
		return nil, errors.Wrap(err, "failed to list database info")
	}

	for _, session := range sessions {
		opts := []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		}

		conn, err := grpc.DialContext(ctx, session.Address, opts...)
		if err != nil {
			fmt.Printf("failed to connect %s(%d), err: %s\n", session.ServerName, session.ServerID, err.Error())
			continue
		}

		if session.ServerName == "rootcoord" || session.ServerName == "mixcoord" {
			clientv2 := rootcoordpb.NewRootCoordClient(conn)
			for _, db := range dbs {
				resp, err := clientv2.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
					Base: &commonpb.MsgBase{
						SourceID: -1,
						TargetID: session.ServerID,
						MsgType:  commonpb.MsgType_ShowCollections,
					},
					DbName: db.GetProto().GetName(),
				})
				if err != nil {
					fmt.Println(err.Error())
					return nil, err
				}
				fmt.Printf("DB %s, ImMeta cnt: %d, Response cnt: %d\n", db.GetProto().GetName(), len(lo.Filter(collections, func(collection *models.Collection, _ int) bool {
					return collection.GetProto().GetDbId() == db.GetProto().GetId()
				})), len(resp.GetCollectionIds()))
				for idx, id := range resp.GetCollectionIds() {
					if _, ok := inMeta[id]; !ok {
						results = append(results, &HealthzCheckReport{
							Msg: fmt.Sprintf("Collection %d not found in meta while loaded", id),
							Extra: map[string]any{
								"collection_id":   id,
								"collection_name": resp.GetCollectionNames()[idx],
							},
						})
					}
				}
			}
		}
	}

	return results, nil
}

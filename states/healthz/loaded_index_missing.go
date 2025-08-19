package healthz

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	metakv "github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/samber/lo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type LoadedIndexMissing struct {
	checkItemBase
}

func newLoadedIndexMissing() *LoadedIndexMissing {
	return &LoadedIndexMissing{
		checkItemBase: checkItemBase{
			name:        "LOADED_INDEX_MISSING",
			description: `Checks whether some loaded missing some scalar indexes`,
		},
	}
}

func (i *LoadedIndexMissing) Check(ctx context.Context, client metakv.MetaKV, basePath string) ([]*HealthzCheckReport, error) {

	segmentIndexes, err := common.ListSegmentIndex(ctx, client, basePath)
	if err != nil {
		return nil, err
	}

	seg2Idx := lo.GroupBy(segmentIndexes, func(segIdx *models.SegmentIndex) int64 {
		return segIdx.GetProto().GetSegmentID()
	})

	sessions, err := common.ListSessions(ctx, client, basePath)
	if err != nil {
		return nil, err
	}

	qnSessions := lo.Filter(sessions, func(sess *models.Session, _ int) bool {
		return sess.ServerName == "querynode"
	})

	type clientWithID struct {
		client querypb.QueryNodeClient
		id     int64
	}
	var wg sync.WaitGroup
	clientCh := make(chan clientWithID, len(qnSessions))
	for _, session := range qnSessions {
		wg.Add(1)
		go func(session *models.Session) {
			defer wg.Done()
			opts := []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithBlock(),
			}

			dialCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			conn, err := grpc.DialContext(dialCtx, session.Address, opts...)
			cancel()
			// ignore bad session
			if err != nil {
				fmt.Printf("failed to connect %s(%d), err: %s\n", session.ServerName, session.ServerID, err.Error())
				return
			}

			clientv2 := querypb.NewQueryNodeClient(conn)
			clientCh <- clientWithID{
				client: clientv2,
				id:     session.ServerID,
			}
		}(session)
	}
	wg.Wait()
	close(clientCh)

	type distResponse struct {
		resp *querypb.GetDataDistributionResponse
		err  error
		id   int64
	}

	respCh := make(chan distResponse, len(qnSessions))

	for idClient := range clientCh {
		wg.Add(1)
		go func(idClient clientWithID) {
			defer wg.Done()
			resp, err := idClient.client.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{
				Base: &commonpb.MsgBase{
					SourceID: -1,
					TargetID: idClient.id,
				},
			})
			respCh <- distResponse{
				resp: resp,
				err:  err,
				id:   idClient.id,
			}
		}(idClient)
	}

	wg.Wait()
	close(respCh)

	var results []*HealthzCheckReport

	for result := range respCh {
		fmt.Println("===========")
		fmt.Printf("ServerID %d\n", result.id)
		if result.err != nil {
			fmt.Println("Error fetching distribution:", result.err.Error())
			continue
		}
		resp := result.resp
		for _, segmentInfo := range resp.GetSegments() {
			idxes := seg2Idx[segmentInfo.GetID()]
			if len(idxes) != len(segmentInfo.GetIndexInfo()) {
				results = append(results, &HealthzCheckReport{
					Item: i.Name(),
					Msg:  fmt.Sprintf("segment %d has %d index, but querynode(%d) reports %d", segmentInfo.GetID(), len(idxes), result.id, len(segmentInfo.GetIndexInfo())),
					Extra: map[string]any{
						"segment_id":     segmentInfo.GetID(),
						"loaded_indexes": segmentInfo.GetIndexInfo(),
						"expected_indexes": lo.Map(idxes, func(idx *models.SegmentIndex, _ int) int64 {
							return idx.GetProto().GetIndexID()
						}),
					},
				})
			}
		}
	}

	return results, nil
}

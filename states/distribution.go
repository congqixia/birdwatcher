package states

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/samber/lo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

type GetDistributionParam struct {
	framework.ParamBase `use:"show segment-loaded-grpc" desc:"list segments loaded information"`
	CollectionID        int64 `name:"collection" default:"0" desc:"collection id to filter with"`
	NodeID              int64 `name:"node" default:"0" desc:"node id to check"`
}

// GetDistributionCommand iterates all querynodes to list distribution.
func (s *InstanceState) GetDistributionCommand(ctx context.Context, p *GetDistributionParam) error {
	// list segment info to get row count information
	segments, err := common.ListSegments(ctx, s.client, s.basePath, func(s *models.Segment) bool {
		return p.CollectionID == 0 || p.CollectionID == s.CollectionID
	})
	if err != nil {
		return err
	}

	id2Segment := lo.SliceToMap(segments, func(s *models.Segment) (int64, *models.Segment) {
		return s.ID, s
	})

	sessions, err := common.ListSessions(ctx, s.client, s.basePath)
	if err != nil {
		return err
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
			if p.NodeID != 0 && session.ServerID != p.NodeID {
				return
			}
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

	var totalSealedCnt int
	var totalSealedRowcount int64

	for result := range respCh {
		fmt.Println("===========")
		fmt.Printf("ServerID %d\n", result.id)
		if result.err != nil {
			fmt.Println("Error fetching distribution:", result.err.Error())
			continue
		}
		resp := result.resp

		// print channel
		for _, channel := range resp.GetChannels() {
			if p.CollectionID != 0 && channel.GetCollection() != p.CollectionID {
				continue
			}
			fmt.Printf("Channel %s, collection: %d, version %d\n", channel.Channel, channel.Collection, channel.Version)
		}

		for _, lv := range resp.GetLeaderViews() {
			if p.CollectionID != 0 && lv.GetCollection() != p.CollectionID {
				continue
			}
			fmt.Printf("Leader view for channel: %s\n", lv.GetChannel())
			growings := lo.Uniq(lo.Union(lv.GetGrowingSegmentIDs(), lo.Keys(lv.GetGrowingSegments())))
			fmt.Printf("Growing segments number: %d , ids: %v\n", len(growings), growings)
		}

		sealedNum := 0
		sealedRowCount := int64(0)

		collSegments := lo.GroupBy(resp.GetSegments(), func(segment *querypb.SegmentVersionInfo) int64 {
			return segment.GetCollection()
		})

		for collection, segments := range collSegments {
			if p.CollectionID != 0 && collection != p.CollectionID {
				continue
			}
			fmt.Printf("------ Collection %d ------\n", collection)
			var collRowCount int64
			for _, segment := range segments {
				segmentInfo := id2Segment[segment.GetID()]
				var rc int64
				if segmentInfo != nil {
					rc = segmentInfo.NumOfRows
				}
				fmt.Printf("SegmentID: %d CollectionID: %d Channel: %s, NumOfRows %d, indexes: %v\n", segment.GetID(), segment.GetCollection(), segment.GetChannel(), rc, segment.FieldJsonIndexStats)
				sealedNum++
				collRowCount += rc
			}
			fmt.Printf("Collection RowCount total %d\n\n", collRowCount)
			sealedRowCount += collRowCount
		}
		fmt.Println("------------------")
		fmt.Printf("Sealed segments number: %d Sealed Row Num: %d\n", sealedNum, sealedRowCount)
		totalSealedCnt += sealedNum
		totalSealedRowcount += sealedRowCount
	}
	fmt.Println("==========================================")
	fmt.Printf("\n#### total loaded sealed segment number: %d, total loaded row count: %d\n", totalSealedCnt, totalSealedRowcount)
	return nil
}

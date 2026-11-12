package show

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/samber/lo"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

type WALRecoveryStorageParam struct {
	framework.ParamBase `use:"show wal-recovery-storage" desc:"display wal recovery storage information from coordinator meta store"`
	Channel             string `name:"channel" default:"" desc:"phsical channel or vchannel name to filter"`
	WALName             string `name:"wal-name" default:"" desc:"a hint of wal name, auto guess if not provided"`
}

func (c *ComponentShow) WalRecoveryStorageCommand(ctx context.Context, p *WALRecoveryStorageParam) error {
	if p.Channel == "" {
		return errors.Errorf("channel is required, use --channel to specify a physical channel or virtual channel")
	}
	pchannel := funcutil.ToPhysicalChannel(p.Channel)

	metas, err := common.ListWALRecoveryStorage(ctx, c.client, c.metaPath, pchannel)
	if err != nil {
		return err
	}

	sessions, err := common.ListSessions(ctx, c.client, c.metaPath)
	if err != nil {
		return err
	}
	sessionMap := lo.SliceToMap(sessions, func(s *models.Session) (int64, *models.Session) { return s.ServerID, s })

	nodeInfo := types.NewStreamingNodeInfoFromProto(metas.Channel.Node)
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetTitle(fmt.Sprintf("WAL Recovery Storage: %s, At StreamingNode: %s, Checkpoints: %s",
		types.NewPChannelInfoFromProto(metas.Channel.Channel).String(),
		formatStreamingNode(nodeInfo, sessionMap),
		common.FormatWALCheckpoint(p.WALName, metas.Checkpoints)))
	t.AppendHeader(table.Row{"Channel", "Partition Count", "Segment Count", "Segments", "Schemas"})
	for _, meta := range metas.VChannels {
		if !strings.HasPrefix(meta.Vchannel, p.Channel) {
			continue
		}
		segments := make([]string, 0, len(metas.Segments[meta.Vchannel]))
		for _, segment := range metas.Segments[meta.Vchannel] {
			segments = append(segments, common.FormatSegmentAssignmentMeta(segment))
		}
		schemas := make([]string, 0, len(meta.CollectionInfo.Schemas))
		for _, schema := range meta.CollectionInfo.Schemas {
			schemas = append(schemas, common.FormatSchema(schema))
		}
		t.AppendRow(table.Row{
			meta.Vchannel,
			fmt.Sprintf("%d", len(meta.CollectionInfo.Partitions)),
			fmt.Sprintf("%d", len(metas.Segments[meta.Vchannel])),
			strings.Join(segments, "\n"),
			strings.Join(schemas, "\n"),
		})
	}
	t.Render()

	if len(metas.RedundantSchemas) > 0 {
		t.AppendHeader(table.Row{"Redundant Schemas"})
		for _, schemas := range metas.RedundantSchemas {
			for _, schema := range schemas {
				json, err := protojson.Marshal(schema)
				if err != nil {
					return err
				}
				t.AppendRow(table.Row{string(json)})
			}
		}
	}

	if len(metas.RedundantSegments) > 0 {
		t.AppendHeader(table.Row{"Redundant Segments"})
		for _, segments := range metas.RedundantSegments {
			for _, segment := range segments {
				json, err := protojson.Marshal(segment)
				if err != nil {
					return err
				}
				t.AppendRow(table.Row{string(json)})
			}
		}
	}
	return nil
}

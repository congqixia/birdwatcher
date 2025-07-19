package repair

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"
)

type CollectionInfoParam struct {
	framework.ParamBase `use:"repair collection-info" desc:"repair collection info"`
	Path                string `name:"filePath" default:"" desc:"path to the collection info file"`
	DatabaseID          int64  `name:"databaseID" default:"0" desc:"database ID to repair"`
	CollectionID        int64  `name:"collectionID" default:"0" desc:"collection ID to repair"`
	CollectionName      string `name:"collectionName" default:"" desc:"collection name to repair"`
	Run                 bool   `name:"run" default:"false" desc:"run the collection info repair command"`
}

type ListModel struct {
	Infos []*CollectionInfoModel `json:"infos"`
}

type CollectionInfoModel struct {
	CollectionID    int64  `json:"collection_id"`
	DbName          string `json:"db_name"`
	CollectionName  string `json:"collection_name"`
	ConsitencyLevel int32  `json:"consistency_level"`
}

func (c *ComponentRepair) CollectionInfoCommand(ctx context.Context, p *CollectionInfoParam) error {

	databases, err := common.ListDatabase(ctx, c.client, c.basePath)
	if err != nil {
		return fmt.Errorf("failed to list databases: %w", err)
	}

	name2db := lo.SliceToMap(databases, func(item *models.Database) (string, *models.Database) {
		return item.GetProto().GetName(), item
	})

	prefix := path.Join(c.basePath, "datacoord-meta", "channel-cp")
	results, _, err := common.ListProtoObjects[msgpb.MsgPosition](ctx, c.client, prefix)
	if err != nil {
		return fmt.Errorf("failed to list checkpoint: %w", err)
	}
	checkpoints := lo.Filter(results, func(cp *msgpb.MsgPosition, _ int) bool {
		return strings.Contains(cp.GetChannelName(), fmt.Sprintf("%d", p.CollectionID))
	})

	channels := make([]*models.Channel, len(checkpoints))
	for _, cp := range checkpoints {
		pchannel, _, version, err := ParseEntity(cp.GetChannelName())
		if err != nil {
			return fmt.Errorf("failed to parse channel name %s: %w", cp.GetChannelName(), err)
		}
		channels[version] = &models.Channel{
			PhysicalName:  pchannel,
			VirtualName:   cp.GetChannelName(),
			StartPosition: cp,
		}
	}

	data, err := os.ReadFile(p.Path)
	if err != nil {
		return err
	}

	var config ListModel

	err = json.Unmarshal(data, &config)
	if err != nil {
		return err
	}

	fmt.Println("Collection Info cnt:", len(config.Infos))
	id2info := lo.SliceToMap(config.Infos, func(item *CollectionInfoModel) (int64, *CollectionInfoModel) {
		return item.CollectionID, item
	})

	var collectionInfo *CollectionInfoModel
	var dbInfo *models.Database
	var ok bool

	if p.Path != "" {
		fmt.Println("Use backup metafield:", p.Path)
		collectionInfo, ok = id2info[p.CollectionID]
		if !ok {
			return fmt.Errorf("collection ID %d not found in the collection info file", p.CollectionID)
		}

		dbInfo, ok = name2db[collectionInfo.DbName]
		if !ok {
			return fmt.Errorf("collection dbname %s not found current instance", collectionInfo.DbName)
		}
	} else {
		fmt.Println("Use manual specified information")
		if p.DatabaseID == 0 {
			return fmt.Errorf("database ID must be specified when collection info file is not provided")
		}
		if p.CollectionName == "" {
			return fmt.Errorf("collection name must be specified when collection info file is not provided")
		}
		dbInfo, ok = lo.Find(databases, func(db *models.Database) bool {
			return db.GetProto().GetId() == p.DatabaseID
		})
		if !ok {
			return fmt.Errorf("database ID %d not found current instance", p.DatabaseID)
		}
		collectionInfo = &CollectionInfoModel{
			CollectionID:    p.CollectionID,
			DbName:          dbInfo.GetProto().GetName(),
			CollectionName:  p.CollectionName,
			ConsitencyLevel: int32(commonpb.ConsistencyLevel_Bounded),
		}
	}

	collPb := &etcdpb.CollectionInfo{
		ID: collectionInfo.CollectionID,
		Schema: &schemapb.CollectionSchema{
			Name:   collectionInfo.CollectionName,
			DbName: dbInfo.GetProto().GetName(),
		},
		ConsistencyLevel: commonpb.ConsistencyLevel(collectionInfo.ConsitencyLevel),
		DbId:             dbInfo.GetProto().GetId(),
		VirtualChannelNames: lo.Map(channels, func(channel *models.Channel, _ int) string {
			if channel == nil {
				return ""
			}
			return channel.VirtualName
		}),
		PhysicalChannelNames: lo.Map(channels, func(channel *models.Channel, _ int) string {
			if channel == nil {
				return ""
			}
			return channel.PhysicalName
		}),
		StartPositions: lo.Map(channels, func(channel *models.Channel, _ int) *commonpb.KeyDataPair {
			if channel == nil || channel.StartPosition == nil {
				return &commonpb.KeyDataPair{
					Key: channel.VirtualName,
				}
			}
			return &commonpb.KeyDataPair{
				Key:  channel.VirtualName,
				Data: channel.StartPosition.MsgID,
			}
		}),
		ShardsNum: int32(len(channels)),
	}

	targetPath := path.Join(c.basePath, "root-coord", "database", "collection-info", fmt.Sprintf("%d", collPb.GetDbId()), fmt.Sprintf("%d", collPb.GetID()))

	if p.Run {
		bs, err := proto.Marshal(collPb)
		if err != nil {
			return fmt.Errorf("failed to marshal collection info: %w", err)
		}
		err = c.client.Save(ctx, targetPath, string(bs))
		if err != nil {
			return fmt.Errorf("failed to put collection info: %w", err)
		}
		fmt.Println("Collection Info repaired successfully.")
	} else {
		fmt.Println("Planned Path:", targetPath)
		fmt.Println("Collection Info:", collPb)
	}

	return nil
}

// 解析字符串格式 {prefix}_{entityID}v{version}
func ParseEntity(str string) (prefix string, entityID int, version int, err error) {
	// 查找最后一个下划线的位置
	lastUnderscoreIndex := strings.LastIndex(str, "_")
	if lastUnderscoreIndex == -1 {
		return "", 0, 0, errors.New("format error: missing underscore")
	}

	// 分割出prefix和后半部分
	prefix = str[:lastUnderscoreIndex]
	suffix := str[lastUnderscoreIndex+1:]

	// 查找版本分隔符 'v'
	vIndex := strings.LastIndex(suffix, "v")
	if vIndex == -1 {
		return "", 0, 0, errors.New("format error: missing 'v' separator")
	}

	// 提取entityID部分和version部分
	entityIDStr := suffix[:vIndex]
	versionStr := suffix[vIndex+1:]

	// 转换entityID
	entityID, err = strconv.Atoi(entityIDStr)
	if err != nil {
		return "", 0, 0, fmt.Errorf("invalid entityID: %w", err)
	}

	// 转换version
	version, err = strconv.Atoi(versionStr)
	if err != nil {
		return "", 0, 0, fmt.Errorf("invalid version: %w", err)
	}

	return prefix, entityID, version, nil
}

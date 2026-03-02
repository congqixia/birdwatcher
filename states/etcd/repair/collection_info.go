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
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
)

type CollectionInfoParam struct {
	framework.ExecutionParam `use:"repair collection-info" desc:"repair collection info"`
	Path                     string `name:"filePath" default:"" desc:"path to the collection info file"`
	DatabaseID               int64  `name:"databaseID" default:"0" desc:"database ID to repair"`
	CollectionID             int64  `name:"collectionID" default:"0" desc:"collection ID to repair"`
	CollectionName           string `name:"collectionName" default:"" desc:"collection name to repair"`
	EnableDynamic            bool   `name:"enableDynamic" default:"false" desc:"enable dynamic collection info repair, default is false"`
	Description              string `name:"desc" default:"" desc:"description"`
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

	fields, _, err := common.ListProtoObjects[schemapb.FieldSchema](ctx, c.client, path.Join(c.basePath, fmt.Sprintf("root-coord/fields/%d", p.CollectionID)))
	if err != nil {
		return fmt.Errorf("failed to list fields: %w", err)
	}

	hasDynamicFields := lo.ContainsBy(fields, func(field *schemapb.FieldSchema) bool {
		return field.IsDynamic
	})

	fmt.Println("Dynamic Schema flag parsed from fields:", hasDynamicFields)

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

	var collectionInfo *CollectionInfoModel
	var dbInfo *models.Database
	var ok bool

	if p.Path != "" {
		fmt.Println("Use backup metafield:", p.Path)
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
			Name:               collectionInfo.CollectionName,
			DbName:             dbInfo.GetProto().GetName(),
			EnableDynamicField: p.EnableDynamic || hasDynamicFields,
			Description:        p.Description,
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

// ParseEntity parses string format {prefix}_{entityID}v{version}
func ParseEntity(str string) (prefix string, entityID int, version int, err error) {
	// find last "_"
	lastUnderscoreIndex := strings.LastIndex(str, "_")
	if lastUnderscoreIndex == -1 {
		return "", 0, 0, errors.New("format error: missing underscore")
	}

	// split parts before and after last underscore
	prefix = str[:lastUnderscoreIndex]
	suffix := str[lastUnderscoreIndex+1:]

	// find version splitter "v"
	vIndex := strings.LastIndex(suffix, "v")
	if vIndex == -1 {
		return "", 0, 0, errors.New("format error: missing 'v' separator")
	}

	// fetch raw string of entityID and version
	entityIDStr := suffix[:vIndex]
	versionStr := suffix[vIndex+1:]

	entityID, err = strconv.Atoi(entityIDStr)
	if err != nil {
		return "", 0, 0, fmt.Errorf("invalid entityID: %w", err)
	}

	version, err = strconv.Atoi(versionStr)
	if err != nil {
		return "", 0, 0, fmt.Errorf("invalid version: %w", err)
	}

	return prefix, entityID, version, nil
}

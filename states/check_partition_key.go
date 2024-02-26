package states

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/milvus-io/birdwatcher/storage"
	"github.com/minio/minio-go/v7"
	"github.com/samber/lo"
)

type CheckPartitionKeyParam struct {
	framework.ParamBase `use:"check-partiton-key" desc:"check partition key field file"`
	StopIfErr           bool   `name:"stopIfErr" default:"true"`
	RootPath            string `name:"rootPath" default:"files"`
}

func (s *InstanceState) CheckPartitionKeyCommand(ctx context.Context, p *CheckPartitionKeyParam) error {
	collections, err := common.ListCollectionsVersion(ctx, s.client, s.basePath, etcdversion.GetVersion())
	if err != nil {
		return err
	}

	client, bucketName, err := getMinioAccess()
	if err != nil {
		return err
	}

	for _, collection := range collections {
		partKeyField, enablePartKey := lo.Find(collection.Schema.Fields, func(field models.FieldSchema) bool {
			return field.IsPartitionKey
		})
		if !enablePartKey {
			continue
		}

		partitions, err := common.ListCollectionPartitions(ctx, s.client, s.basePath, collection.ID)
		if err != nil {
			continue
		}

		partIdx := lo.SliceToMap(partitions, func(partition *models.Partition) (int64, uint32) {
			splits := strings.Split(partition.Name, "_")
			if len(splits) < 2 {
				// continue
			}
			index, err := strconv.ParseInt(splits[len(splits)-1], 10, 64)
			if err != nil {
				return -1, 0
			}
			if (index >= int64(len(partitions))) || (index < 0) {
				return -1, 0
			}
			return partition.ID, uint32(index)
		})
		segments, err := common.ListSegmentsVersion(ctx, s.client, s.basePath, etcdversion.GetVersion(), func(segment *models.Segment) bool {
			return segment.CollectionID == collection.ID
		})

		if err != nil {
			return err
		}

		var found bool

		for _, segment := range segments {
			if segment.State == models.SegmentStateDropped || segment.State == models.SegmentStateSegmentStateNone {
				continue
			}
			binlogs := segment.GetBinlogs()
			partKeyBinlog, ok := lo.Find(binlogs, func(binlog *models.FieldBinlog) bool {
				return binlog.FieldID == partKeyField.FieldID
			})
			if !ok {
				continue
			}
			targetIdx := partIdx[segment.PartitionID]
			for _, binlog := range partKeyBinlog.Binlogs {
				filePath := strings.Replace(binlog.LogPath, "ROOT_PATH", p.RootPath, -1)
				func(filePath string) {
					result, err := client.GetObject(ctx, bucketName, filePath, minio.GetObjectOptions{})
					if err != nil {
						fmt.Println(err.Error())
					}

					bReader, _, err := storage.NewBinlogReader(result)
					defer bReader.Close()
					var target int64
					switch partKeyField.DataType {
					case models.DataTypeInt64:
						var data []int64
						for ; err == nil; data, err = bReader.NextInt64EventReader(result) {
							for _, val := range data {
								h, _ := Hash32Int64(val)
								if (h % uint32(len(partitions))) != targetIdx {
									found = true
									return
								}
							}
						}

					case models.DataTypeVarChar:
						var data []string
						for ; err == nil; data, err = bReader.NextVarcharEventReader(result) {
							for _, val := range data {
								h := HashString2Uint32(val)
								if (h % uint32(len(partitions))) != targetIdx {
									found = true
									return
								}
							}
						}
					}
					if target > 0 {
						// fmt.Printf("Segment: %d, Path: %s, mismatch found: %d\n", segment.ID, filePath, target)
						found = true
						return
					}
				}(filePath)
				if found {
					break
				}
			}
		}
		if found {
			fmt.Printf("Collection found %d %s\n", collection.ID, collection.Schema.Name)
		}
	}
	return nil
}

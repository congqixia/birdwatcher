package states

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/gosuri/uilive"
	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/storage"
	binlogv1 "github.com/milvus-io/birdwatcher/storage/binlog/v1"
	storagecommon "github.com/milvus-io/birdwatcher/storage/common"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

type CheckPartitionKeyParam struct {
	OSSOverrideParam
	framework.ParamBase `use:"check-partiton-key" desc:"check partition key field file"`
	Storage             string `name:"storage" default:"auto" desc:"storage service configuration mode"`
	StopIfErr           bool   `name:"stopIfErr" default:"true"`
	OutputPrimaryKey    bool   `name:"outputPK" default:"true" desc:"print error record primary key info in stdout mode"`
	MinioAddress        string `name:"minioAddr" default:"" desc:"the minio address to override, leave empty to use milvus.yaml value"`
	OutputFormat        string `name:"outputFmt" default:"stdout"`

	CollectionID int64 `name:"collection" default:"0" desc:"target collection to scan, default scan all partition key collections"`
}

var errQuickExit = errors.New("quick exit")

// TODO refactor this command using new scan API

func (s *InstanceState) CheckPartitionKeyCommand(ctx context.Context, p *CheckPartitionKeyParam) error {
	collections, err := common.ListCollections(ctx, s.client, s.basePath, func(collection *models.Collection) bool {
		return p.CollectionID == 0 || collection.GetProto().ID == p.CollectionID
	})
	if err != nil {
		return err
	}

	var rootPath string

	resolvedStore, err := s.getObjectStoreWithOverrides(ctx, p.OSSOverrideParam, p.MinioAddress, false)
	if err != nil {
		return err
	}
	rootPath = resolvedStore.RootPath

	type suspectCollection struct {
		collection   *models.Collection
		partitions   []*models.Partition
		partIndex    map[int64]uint32 // partition id to hash index
		idName       map[int64]string
		partKeyField models.FieldSchema
		pkField      models.FieldSchema
		tsField      models.FieldSchema
	}

	var suspectCollections []*suspectCollection

	for _, info := range collections {
		collection := info.GetProto()
		partKeyField, enablePartKey := lo.Find(collection.Schema.Fields, func(field *schemapb.FieldSchema) bool {
			return field.IsPartitionKey
		})
		if !enablePartKey {
			continue
		}
		pkField, _ := lo.Find(collection.Schema.Fields, func(field *schemapb.FieldSchema) bool {
			return field.IsPrimaryKey
		})
		tsField, _ := lo.Find(collection.Schema.Fields, func(field *schemapb.FieldSchema) bool {
			return field.FieldID == 1
		})

		partitions, err := common.ListCollectionPartitions(ctx, s.client, s.basePath, collection.ID)
		if err != nil {
			continue
		}

		partIdx := lo.SliceToMap(partitions, func(partition *models.Partition) (int64, uint32) {
			splits := strings.Split(partition.GetProto().PartitionName, "_")
			if len(splits) < 2 {
				return -1, 0
			}
			index, err := strconv.ParseInt(splits[len(splits)-1], 10, 64)
			if err != nil {
				return -1, 0
			}
			if (index >= int64(len(partitions))) || (index < 0) {
				return -1, 0
			}
			return partition.GetProto().PartitionID, uint32(index)
		})
		idName := lo.SliceToMap(partitions, func(partition *models.Partition) (int64, string) {
			return partition.GetProto().PartitionID, partition.GetProto().PartitionName
		})

		suspectCollections = append(suspectCollections, &suspectCollection{
			collection:   info,
			partitions:   partitions,
			partIndex:    partIdx,
			idName:       idName,
			partKeyField: models.NewFieldSchemaFromBase(partKeyField),
			pkField:      models.NewFieldSchemaFromBase(pkField),
			tsField:      models.NewFieldSchemaFromBase(tsField),
		})
	}

	for _, susCol := range suspectCollections {
		collection := susCol.collection.GetProto()
		partitions := susCol.partitions
		partKeyField := susCol.partKeyField
		partIdx := susCol.partIndex
		pkField, _ := susCol.collection.GetPKField()
		idField := lo.SliceToMap(collection.Schema.Fields, func(field *schemapb.FieldSchema) (int64, *schemapb.FieldSchema) {
			return field.FieldID, field
		})

		fmt.Printf("Start to check collection %s id = %d\n", collection.Schema.Name, collection.ID)

		segments, err := common.ListSegments(ctx, s.client, s.basePath, func(segment *models.Segment) bool {
			return segment.CollectionID == collection.ID
		})
		if err != nil {
			return err
		}

		var collectionErrs int
		var found bool

		fmt.Printf("Partition number: %d, Segment number %d\n", len(partitions), len(segments))
		progressDisplay := uilive.New()
		progressFmt := "Scan segment ... %d%%(%d/%d) %s\n"
		progressDisplay.Start()
		fmt.Fprintf(progressDisplay, progressFmt, 0, 0, len(segments), "")

		for idx, segment := range segments {
			if segment.State == commonpb.SegmentState_Dropped || segment.State == commonpb.SegmentState_NotExist {
				continue
			}
			var errCnt int
			err := func() error {
				var f *os.File
				var pqWriter *binlogv1.ParquetWriter
				selector := func(_ int64) bool { return true }
				switch p.OutputFormat {
				case "stdout":
					selector = func(field int64) bool { return field == partKeyField.FieldID }
				case "json-pk":
					selector = func(field int64) bool { return field == partKeyField.FieldID }
					fallthrough
				case "json":
					f, err = os.Create(fmt.Sprintf("%d-%d.json", collection.ID, segment.ID))
					if err != nil {
						return err
					}
				case "parquet":
					f, err = os.Create(fmt.Sprintf("%d-%d.parquet", collection.ID, segment.ID))
					if err != nil {
						return err
					}
					pqWriter = binlogv1.NewParquetWriter(susCol.collection)
				}
				deltalog, err := s.DownloadDeltalogs(ctx, resolvedStore.Store, rootPath, susCol.collection, segment)
				if err != nil {
					return err
				}

				s.ScanBinlogs(ctx, resolvedStore.Store, rootPath, susCol.collection, segment, selector, func(readers map[int64]*binlogv1.BinlogReader) {
					targetIndex := partIdx[segment.PartitionID]
					iter, err := NewBinlogIterator(susCol.collection, readers)
					if err != nil {
						fmt.Println("failed to create iterator", err.Error())
						return
					}

					err = iter.Range(func(rowID, ts int64, pk storagecommon.PrimaryKey, data map[int64]any) error {
						deleted := false
						deltalog.Range(func(delPk storagecommon.PrimaryKey, delTs uint64) bool {
							if delPk.EQ(pk) && ts < int64(delTs) {
								deleted = true
								return false
							}
							return true
						})
						if deleted {
							return nil
						}

						partKeyValue := data[partKeyField.FieldID]
						var hashVal uint32
						switch partKeyField.DataType {
						case models.DataTypeInt64:
							hashVal, _ = Hash32Int64(partKeyValue.(int64))
						case models.DataTypeVarChar, models.DataTypeString:
							hashVal = HashString2Uint32(partKeyValue.(string))
						default:
							return errors.Newf("unexpected partition key field type %v", partKeyField.DataType)
						}
						if (hashVal % uint32(len(partitions))) == targetIndex {
							return nil
						}
						errCnt++
						found = true

						output := lo.MapKeys(data, func(v any, k int64) string {
							return idField[k].Name
						})
						output[pkField.Name] = pk.GetValue()
						switch p.OutputFormat {
						case "stdout":
							if p.OutputPrimaryKey {
								fmt.Printf("PK %v partition does not follow partition key rule (%s=%v)\n", pk.GetValue(), partKeyField.Name, partKeyValue)
							}
							if p.StopIfErr {
								return errQuickExit
							}
						case "json", "json-pk":
							bs, err := json.Marshal(output)
							if err != nil {
								fmt.Println(err.Error())
								return err
							}
							f.Write(bs)
							f.Write([]byte("\n"))
						case "parquet":
							data[0] = rowID
							data[1] = ts
							data[pkField.FieldID] = pk.GetValue()
							writeParquetData(susCol.collection, pqWriter, rowID, ts, pk, output)
						}
						return nil
					})
					if err != nil && !errors.Is(err, errQuickExit) {
						fmt.Println(err.Error())
					}
				})
				return nil
			}()
			if err != nil && !errors.Is(err, errQuickExit) {
				return err
			}
			if p.StopIfErr && found {
				break
			}
			progress := (idx + 1) * 100 / len(segments)
			status := fmt.Sprintf("%d [%s]", segment.ID, colorReady.Sprint("done"))
			if errCnt > 0 {
				collectionErrs += errCnt
				status = fmt.Sprintf("%d [%s](%d)", segment.ID, colorError.Sprint("error"), errCnt)
			}

			fmt.Fprintf(progressDisplay, progressFmt, progress, idx+1, len(segments), status)
		}
		progressDisplay.Stop()
		fmt.Println()
		if p.StopIfErr {
			if found {
				fmt.Printf("Collection %s found partition key error\n", collection.Schema.Name)
			} else {
				fmt.Printf("Collection %s all data OK!\n", collection.Schema.Name)
			}
		} else {
			fmt.Printf("Collection %s found %d partition key error\n", collection.Schema.Name, collectionErrs)
		}
	}
	return nil
}

func (s *InstanceState) DownloadDeltalogs(ctx context.Context, store oss.ObjectStore, rootPath string, collection *models.Collection, segment *models.Segment) (*storage.DeltaData, error) {
	pkField, has := lo.Find(collection.GetProto().Schema.Fields, func(field *schemapb.FieldSchema) bool {
		return field.IsPrimaryKey
	})
	if !has {
		return nil, errors.New("pk not found")
	}
	data := storage.NewDeltaData(pkField.DataType, 0)
	for _, delFieldBinlog := range segment.GetDeltalogs() {
		for _, binlog := range delFieldBinlog.Binlogs {
			filePath := oss.ResolveObjectKey(rootPath, binlog.LogPath)
			result, err := store.Open(ctx, filePath)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}

			reader, err := storage.NewDeltalogReader(result)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}

			var deltaData *storage.DeltaData
			for err == nil {
				deltaData, err = reader.NextEventReader(pkField.DataType)
				if err == nil {
					err = data.Merge(deltaData)
					if err != nil {
						return nil, err
					}
				}
			}
			if err != io.EOF {
				return nil, err
			}
		}
	}
	return data, nil
}

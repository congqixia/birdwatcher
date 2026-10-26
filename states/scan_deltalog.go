package states

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/expr-lang/expr"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/storage"
	storagecommon "github.com/milvus-io/birdwatcher/storage/common"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

type ScanDeltalogParams struct {
	OSSOverrideParam
	framework.ParamBase `use:"scan-deltalog" desc:"scan deltalog to check delta data"`
	CollectionID        int64    `name:"collection" default:"0"`
	SegmentID           int64    `name:"segment" default:"0"`
	Fields              []string `name:"fields"`
	Expr                string   `name:"expr"`
	MinioAddress        string   `name:"minioAddr"`
	SkipBucketCheck     bool     `name:"skipBucketCheck" default:"false" desc:"skip bucket exist check due to permission issue"`
	Action              string   `name:"action" default:"count"`
	Limit               int64    `name:"limit" default:"0" desc:"limit the scan line number if action is locate"`
	IncludeUnhealthy    bool     `name:"includeUnhealthy" default:"false" desc:"also check dropped segments"`
}

func (s *InstanceState) ScanDeltalogCommand(ctx context.Context, p *ScanDeltalogParams) error {
	collection, err := common.GetCollectionByIDVersion(ctx, s.client, s.basePath, p.CollectionID)
	if err != nil {
		return err
	}
	fmt.Println("=== Checking collection schema ===")
	pkField, ok := collection.GetPKField()
	if !ok {
		return errors.New("pk field not found")
	}
	fmt.Printf("PK Field [%d] %s\n", pkField.FieldID, pkField.Name)

	fieldsMap := make(map[string]struct{})
	for _, field := range p.Fields {
		fieldsMap[field] = struct{}{}
	}

	fields := make(map[int64]models.FieldSchema) // make([]models.FieldSchema, 0, len(p.Fields))

	for _, fieldSchema := range collection.GetProto().Schema.Fields {
		// timestamp field id
		if fieldSchema.FieldID == 1 {
			fields[fieldSchema.FieldID] = models.NewFieldSchemaFromBase(fieldSchema)
			continue
		}
		if _, ok := fieldsMap[fieldSchema.Name]; ok {
			fmt.Printf("Output Field %s field id %d\n", fieldSchema.Name, fieldSchema.FieldID)
			fields[fieldSchema.FieldID] = models.NewFieldSchemaFromBase(fieldSchema)
		}
	}

	segments, err := common.ListSegments(ctx, s.client, s.basePath, func(s *models.Segment) bool {
		return (p.SegmentID == 0 || p.SegmentID == s.ID) &&
			p.CollectionID == s.CollectionID &&
			(p.IncludeUnhealthy || s.State != commonpb.SegmentState_Dropped)
	})
	if err != nil {
		return err
	}

	resolvedStore, err := s.getObjectStoreWithOverrides(ctx, p.OSSOverrideParam, p.MinioAddress, p.SkipBucketCheck)
	if err != nil {
		fmt.Println("Failed to create client,", err.Error())
		return err
	}
	rootPath := resolvedStore.RootPath

	fmt.Printf("=== start to execute \"%s\" task with filter expresion: \"%s\" ===\n", p.Action, p.Expr)

	// prepare action dataset
	// count
	var count int64
	// locate do nothing

	var process func(pk storagecommon.PrimaryKey, ts uint64, segment *models.Segment, logPath string, offset int64) bool

	switch p.Action {
	case "count":
		process = func(pk storagecommon.PrimaryKey, ts uint64, segment *models.Segment, logPath string, offset int64) bool {
			count++
			return true
		}
	case "locate":
		process = func(pk storagecommon.PrimaryKey, ts uint64, segment *models.Segment, logPath string, offset int64) bool {
			log.Printf("Entry found in segment %d, level = %s, log file = %s, offset = %d, pk = %v, ts = %d\n", segment.ID, segment.Level.String(), logPath, offset, pk.GetValue(), ts)
			count++
			return p.Limit <= 0 || count < p.Limit
		}
	}

	getObject := func(binlogPath string) (storagecommon.ReadSeeker, error) {
		logPath := oss.ResolveObjectKey(rootPath, binlogPath)
		return resolvedStore.Store.Open(ctx, logPath)
	}
	for _, segment := range segments {
		for _, fieldBinlogs := range segment.GetDeltalogs() {
			for _, deltaBinlog := range fieldBinlogs.Binlogs {
				deltaObj, err := getObject(deltaBinlog.LogPath)
				if err != nil {
					return err
				}
				reader, err := storage.NewDeltalogReader(deltaObj)
				if err != nil {
					return err
				}
				deltaData, err := reader.NextEventReader(schemapb.DataType(pkField.DataType))
				if err != nil {
					return err
				}
				offset := int64(0)
				deltaData.Range(func(pk storagecommon.PrimaryKey, ts uint64) bool {
					defer func() {
						offset++
					}()
					if len(p.Expr) != 0 {
						env := map[string]any{
							"$pk":        pk.GetValue(),
							"$timestamp": ts,
						}
						program, err := expr.Compile(p.Expr, expr.Env(env))
						if err != nil {
							return false
						}

						output, err := expr.Run(program, env)
						if err != nil {
							fmt.Println("failed to run expression, err: ", err.Error())
						}

						match, ok := output.(bool)
						if !ok {
							fmt.Println("expr not return bool value")
							return false
						}

						if !match {
							return true
						}
					}

					process(pk, ts, segment, deltaBinlog.LogPath, offset)
					return true
				})
			}
		}
	}

	switch strings.ToLower(p.Action) {
	case "count":
		fmt.Printf("Total %d entries found\n", count)
	default:
	}

	return nil
}

package states

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"github.com/samber/lo"
	"go.uber.org/atomic"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/storage"
	storagecommon "github.com/milvus-io/birdwatcher/storage/common"
	"github.com/milvus-io/birdwatcher/storage/tasks"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type ScanBinlogParams struct {
	framework.ParamBase `use:"scan-binlog" desc:"scan binlog to check data"`
	CollectionID        int64    `name:"collection" default:"0"`
	PartitionID         int64    `name:"partition" default:"0"`
	SegmentID           int64    `name:"segment" default:"0"`
	Fields              []string `name:"fields"`
	Expr                string   `name:"expr"`
	MinioAddress        string   `name:"minioAddr"`
	SkipBucketCheck     bool     `name:"skipBucketCheck" default:"false" desc:"skip bucket exist check due to permission issue"`
	Action              string   `name:"action" default:"count"`
	IgnoreDelete        bool     `name:"ignoreDelete" default:"false" desc:"ignore delete logic"`
	IncludeUnhealthy    bool     `name:"includeUnhealthy" default:"false" desc:"also check dropped segments"`
	WorkerNum           int64    `name:"workerNum" default:"4" desc:"worker num"`
	OutputLimit         int64    `name:"outputLimit" default:"10" desc:"output limit"`
	DumpFile            string   `name:"dumpFile" default:""`
}

func (s *InstanceState) ScanBinlogCommand(ctx context.Context, p *ScanBinlogParams) error {
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

	fields := make(map[int64]*schemapb.FieldSchema)

	for _, fieldSchema := range collection.GetProto().Schema.Fields {
		// timestamp field id
		if fieldSchema.FieldID == 1 {
			fields[fieldSchema.FieldID] = fieldSchema
			continue
		}
		if fieldSchema.IsPrimaryKey {
			fmt.Printf("Output PK Field %s field id %d\n", fieldSchema.Name, fieldSchema.FieldID)
			fields[fieldSchema.FieldID] = fieldSchema
			continue
		}
		if _, ok := fieldsMap[fieldSchema.Name]; ok {
			fmt.Printf("Output Field %s field id %d\n", fieldSchema.Name, fieldSchema.FieldID)
			fields[fieldSchema.FieldID] = fieldSchema
		}
	}

	segments, err := common.ListSegments(ctx, s.client, s.basePath, func(s *models.Segment) bool {
		return (p.SegmentID == 0 || p.SegmentID == s.ID) &&
			p.CollectionID == s.CollectionID &&
			(p.PartitionID == 0 || p.PartitionID == s.PartitionID) &&
			(p.IncludeUnhealthy || s.State != commonpb.SegmentState_Dropped)
	})
	if err != nil {
		return err
	}

	params := []oss.MinioConnectParam{oss.WithSkipCheckBucket(p.SkipBucketCheck)}
	if p.MinioAddress != "" {
		params = append(params, oss.WithMinioAddr(p.MinioAddress))
	}

	minioClient, bucketName, rootPath, err := s.GetMinioClientFromCfg(ctx, params...)
	if err != nil {
		fmt.Println("Failed to create client,", err.Error())
		return err
	}

	fmt.Printf("=== start to execute \"%s\" task with filter expresion: \"%s\" ===\n", p.Action, p.Expr)
	fmt.Printf("=== worker num: %d, skip delete: %t ===\n", p.WorkerNum, p.IgnoreDelete)

	var scanTask tasks.ScanTask
	switch strings.ToLower(p.Action) {
	case "count":
		scanTask = tasks.NewCountTask()
	case "locate":
		scanTask = tasks.NewLocateTask(p.OutputLimit, pkField)
	case "dedup":
		scanTask = tasks.NewDedupTask(p.OutputLimit, pkField, p.DumpFile)
	default:
		return errors.Newf("unknown action: %s", p.Action)
	}

	var exprFilter *storage.ExprFilter
	if p.Expr != "" {
		exprFilter, err = storage.NewExprFilter(fields, p.Expr)
		if err != nil {
			return err
		}
	}

	getObject := func(binlogPath string) (storagecommon.ReadSeeker, error) {
		logPath := strings.ReplaceAll(binlogPath, "ROOT_PATH", rootPath)
		return minioClient.GetObject(ctx, bucketName, logPath, minio.GetObjectOptions{})
	}

	l0Segments := lo.Filter(segments, func(segment *models.Segment, _ int) bool {
		return segment.Level == datapb.SegmentLevel_L0
	})
	normalSegments := lo.Filter(segments, func(segment *models.Segment, _ int) bool {
		return segment.Level != datapb.SegmentLevel_L0
	})

	l0DeleteRecords := make(map[any]uint64) // pk => ts

	addDeltaRecords := func(segment *models.Segment, recordMap map[any]uint64) error {
		for _, deltaFieldBinlog := range segment.GetDeltalogs() {
			for _, deltaBinlog := range deltaFieldBinlog.Binlogs {
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
				deltaData.Range(func(pk storagecommon.PrimaryKey, ts uint64) bool {
					if old, ok := recordMap[ts]; !ok || ts > old {
						recordMap[pk.GetValue()] = ts
					}
					return true
				})
			}
		}
		return nil
	}

	for _, segment := range l0Segments {
		addDeltaRecords(segment, l0DeleteRecords)
	}
	loEntryFilter := storage.NewDeltalogFilter(l0DeleteRecords)

	workFn := func(segment *models.Segment) error {
		deletedRecords := make(map[any]uint64) // pk => ts
		addDeltaRecords(segment, deletedRecords)
		deltalogFilter := storage.NewDeltalogFilter(deletedRecords)

		filters := []storage.EntryFilter{loEntryFilter, deltalogFilter}
		if exprFilter != nil {
			filters = append(filters, exprFilter)
		}

		iter := storage.NewSegmentIterator(segment,
			collection.GetProto().GetSchema(),
			filters,
			fields,
			getObject,
			scanTask)

		return iter.Range(ctx)
	}

	var wg sync.WaitGroup
	wg.Add(int(p.WorkerNum))
	taskCh := make(chan *models.Segment)
	errCh := make(chan error, 1)

	num := atomic.NewInt64(0)

	for i := 0; i < int(p.WorkerNum); i++ {
		go func() {
			defer wg.Done()
			for {
				segment, ok := <-taskCh
				if !ok {
					return
				}
				err := workFn(segment)
				if err != nil && !errors.Is(err, io.EOF) {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				fmt.Printf("%d/%d done, current counter: %d\n", num.Inc(), len(normalSegments), scanTask.Counter())
			}
		}()
	}

	for _, segment := range normalSegments {
		select {
		case taskCh <- segment:
		case err = <-errCh:
		}
		if err != nil {
			break
		}
	}
	close(taskCh)
	wg.Wait()

	if err != nil {
		return err
	}

	scanTask.Summary()

	return nil
}

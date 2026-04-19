package states

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	binlogv1 "github.com/milvus-io/birdwatcher/storage/binlog/v1"
	storagecommon "github.com/milvus-io/birdwatcher/storage/common"
)

type InspectParquetParam struct {
	framework.ParamBase `use:"inspect-parquet" desc:"inspect parquet file metadata and optionally sample rows"`
	FilePath            string `name:"file" default:"" desc:"local parquet file path to inspect"`
	SegmentID           int64  `name:"segment" default:"0" desc:"segment ID to inspect binlogs from remote storage"`
	FieldID             int64  `name:"field" default:"0" desc:"only inspect binlogs of this field ID (0 means all fields)"`
	MetadataOnly        bool   `name:"metadataOnly" default:"true" desc:"print metadata only; set to false to also sample rows"`
	SampleRows          int64  `name:"sampleRows" default:"10" desc:"number of rows to sample when metadataOnly=false"`
	ShowRowGroups       bool   `name:"showRowGroups" default:"false" desc:"print per-row-group statistics"`
	MinioAddress        string `name:"minioAddr" default:"" desc:"override minio address"`
	SkipBucketCheck     bool   `name:"skipBucketCheck" default:"false" desc:"skip bucket existence check"`
}

// InspectParquetCommand inspects parquet metadata (and optionally samples rows) from either a local
// parquet file or from a segment's remote binlog files.
func (s *InstanceState) InspectParquetCommand(ctx context.Context, p *InspectParquetParam) error {
	if p.FilePath == "" && p.SegmentID == 0 {
		return errors.New("either --file or --segment must be provided")
	}
	if p.FilePath != "" && p.SegmentID != 0 {
		return errors.New("--file and --segment are mutually exclusive")
	}

	if p.FilePath != "" {
		return s.inspectLocalParquet(ctx, p)
	}
	return s.inspectSegmentParquet(ctx, p)
}

func (s *InstanceState) inspectLocalParquet(ctx context.Context, p *InspectParquetParam) error {
	f, err := openBackupFile(p.FilePath)
	if err != nil {
		return err
	}
	defer f.Close()

	pqReader, err := file.NewParquetReader(f)
	if err != nil {
		return errors.Wrapf(err, "failed to open parquet file %s", p.FilePath)
	}
	defer pqReader.Close()

	return printParquetFile(ctx, pqReader, p.FilePath, p.MetadataOnly, p.SampleRows, p.ShowRowGroups)
}

func (s *InstanceState) inspectSegmentParquet(ctx context.Context, p *InspectParquetParam) error {
	segments, err := common.ListSegments(ctx, s.client, s.basePath, func(seg *models.Segment) bool {
		return seg.ID == p.SegmentID
	})
	if err != nil {
		return err
	}
	if len(segments) == 0 {
		return errors.Newf("segment %d not found", p.SegmentID)
	}
	segment := segments[0]
	fmt.Printf("Segment %d: collection=%d partition=%d storageVersion=%d\n",
		segment.ID, segment.CollectionID, segment.PartitionID, segment.StorageVersion)

	params := []oss.MinioConnectParam{oss.WithSkipCheckBucket(p.SkipBucketCheck)}
	if p.MinioAddress != "" {
		params = append(params, oss.WithMinioAddr(p.MinioAddress))
	}
	minioClient, bucketName, rootPath, err := s.GetMinioClientFromCfg(ctx, params...)
	if err != nil {
		return err
	}

	for _, fieldBinlog := range segment.GetBinlogs() {
		if p.FieldID != 0 && fieldBinlog.FieldID != p.FieldID {
			continue
		}
		for _, binlog := range fieldBinlog.Binlogs {
			logPath := strings.ReplaceAll(binlog.LogPath, "ROOT_PATH", rootPath)
			fmt.Printf("\n===== Field %d | %s =====\n", fieldBinlog.FieldID, logPath)
			if err := inspectRemoteBinlog(ctx, minioClient, bucketName, logPath, segment.StorageVersion, p.MetadataOnly, p.SampleRows, p.ShowRowGroups); err != nil {
				fmt.Printf("failed to inspect %s: %s\n", logPath, err.Error())
			}
		}
	}
	return nil
}

func inspectRemoteBinlog(ctx context.Context, minioClient *minio.Client, bucketName, logPath string, storageVersion int64, metadataOnly bool, sampleRows int64, showRowGroups bool) error {
	obj, err := minioClient.GetObject(ctx, bucketName, logPath, minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	defer obj.Close()

	pqReader, err := openBinlogParquet(obj, storageVersion)
	if err != nil {
		return err
	}
	defer pqReader.Close()

	return printParquetFile(ctx, pqReader, path.Base(logPath), metadataOnly, sampleRows, showRowGroups)
}

func openBinlogParquet(r storagecommon.ReadSeeker, storageVersion int64) (*file.Reader, error) {
	switch storageVersion {
	case 2:
		return file.NewParquetReader(r)
	case 0, 1:
		br, err := binlogv1.NewBinlogReader(r)
		if err != nil {
			return nil, err
		}
		return br.NextParquetReader()
	default:
		return nil, errors.Newf("unsupported storage version: %d", storageVersion)
	}
}

func printParquetFile(ctx context.Context, pqReader *file.Reader, name string, metadataOnly bool, sampleRows int64, showRowGroups bool) error {
	printParquetMetadata(pqReader, name, showRowGroups)
	if metadataOnly {
		return nil
	}
	return samplePqRows(ctx, pqReader, sampleRows)
}

func printParquetMetadata(pqReader *file.Reader, name string, showRowGroups bool) {
	md := pqReader.MetaData()
	fmt.Printf("--- Parquet metadata: %s ---\n", name)
	fmt.Printf("NumRows: %d | NumRowGroups: %d\n", pqReader.NumRows(), pqReader.NumRowGroups())
	fmt.Printf("CreatedBy: %s | Version: %d\n", md.GetCreatedBy(), md.Version())

	schema := md.Schema
	fmt.Printf("Parquet Columns (%d):\n", schema.NumColumns())
	for i := 0; i < schema.NumColumns(); i++ {
		col := schema.Column(i)
		fmt.Printf("  [%d] %s (physical=%s logical=%s)\n",
			i, col.Name(), col.PhysicalType().String(), col.LogicalType().String())
	}

	if arrReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator); err == nil {
		if arrowSchema, err := arrReader.Schema(); err == nil {
			fmt.Printf("Arrow Schema:\n%s\n", arrowSchema.String())
		}
	}

	if showRowGroups {
		for rg := 0; rg < pqReader.NumRowGroups(); rg++ {
			rgMd := md.RowGroup(rg)
			fmt.Printf("RowGroup %d: NumRows=%d TotalByteSize=%d\n",
				rg, rgMd.NumRows(), rgMd.TotalByteSize())
		}
	}
}

func samplePqRows(ctx context.Context, pqReader *file.Reader, limit int64) error {
	if limit <= 0 {
		return nil
	}
	arrReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.DefaultAllocator)
	if err != nil {
		return err
	}
	rr, err := arrReader.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return err
	}
	defer rr.Release()

	fmt.Printf("--- Sample up to %d rows ---\n", limit)
	var printed int64
	for printed < limit && rr.Next() {
		rec := rr.Record()
		n := printRecordRows(rec, printed, limit-printed)
		printed += n
		if n == 0 {
			break
		}
	}
	if printed == 0 {
		fmt.Println("(no rows)")
	}
	return nil
}

func printRecordRows(rec arrow.Record, startIdx, limit int64) int64 {
	rows := min(rec.NumRows(), limit)
	cols := int(rec.NumCols())
	names := make([]string, cols)
	for i := range cols {
		names[i] = rec.ColumnName(i)
	}
	for i := range rows {
		parts := make([]string, cols)
		for c := range cols {
			parts[c] = fmt.Sprintf("%s=%s", names[c], arrowCellString(rec.Column(c), int(i)))
		}
		fmt.Printf("[%d] %s\n", startIdx+i, strings.Join(parts, ", "))
	}
	return rows
}

func arrowCellString(arr arrow.Array, idx int) string {
	if arr.IsNull(idx) {
		return "<null>"
	}
	// arrow.Array.String() renders the whole array; for per-cell use ValueStr when available.
	type valueStr interface {
		ValueStr(int) string
	}
	if v, ok := arr.(valueStr); ok {
		return v.ValueStr(idx)
	}
	return arr.String()
}

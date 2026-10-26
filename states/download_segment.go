package states

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"github.com/manifoldco/promptui"
	"github.com/minio/minio-go/v7"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type DownloadSegmentParam struct {
	OSSOverrideParam
	framework.ParamBase `use:"download-segment" desc:"download segment file with provided segment id"`
	MinioAddress        string `name:"minioAddr" default:"" desc:"the minio address to override, leave empty to use milvus.yaml value"`
	SegmentID           int64  `name:"segment" default:"0" desc:"segment id to download"`
}

func (s *InstanceState) DownloadSegmentCommand(ctx context.Context, p *DownloadSegmentParam) error {
	segments, err := common.ListSegments(ctx, s.client, s.basePath, func(s *models.Segment) bool {
		return s.ID == p.SegmentID
	})
	if err != nil {
		return err
	}

	resolvedStore, err := s.getObjectStoreWithOverrides(ctx, p.OSSOverrideParam, p.MinioAddress, false)
	if err != nil {
		return err
	}

	folder := fmt.Sprintf("dlsegment_%s", time.Now().Format("20060102150406"))
	for _, segment := range segments {
		err := downloadSegment(ctx, resolvedStore.Store, resolvedStore.RootPath, segment, folder)
		if err != nil {
			return err
		}
	}

	return nil
}

func downloadSegment(ctx context.Context, store oss.ObjectStore, rootPath string, segment *models.Segment, folderPath string) error {
	p := path.Join(folderPath, fmt.Sprintf("%d", segment.ID))
	if _, err := os.Stat(p); errors.Is(err, os.ErrNotExist) {
		err := os.MkdirAll(p, os.ModePerm)
		if err != nil {
			fmt.Println("Failed to create folder,", err.Error())
			return err
		}
	}

	fmt.Printf("Downloading Segment: %d ...\n", segment.ID)

	for _, fieldBinlog := range segment.GetBinlogs() {
		folder := fmt.Sprintf("%s/%d", p, fieldBinlog.FieldID)
		err := os.MkdirAll(folder, 0o777)
		if err != nil {
			fmt.Println("Failed to create sub-folder", err.Error())
			return err
		}

		for _, binlog := range fieldBinlog.Binlogs {
			logPath := oss.ResolveObjectKey(rootPath, binlog.LogPath)
			obj, err := store.Open(ctx, logPath)
			if err != nil {
				fmt.Printf("failed to download file filePath = \"%s\", err: %s\n", logPath, err.Error())
				return err
			}

			name := path.Base(logPath)

			f, err := os.Create(path.Join(folder, name))
			if err != nil {
				fmt.Println("failed to open file")
				return err
			}
			w := bufio.NewWriter(f)
			r := bufio.NewReader(obj)
			io.Copy(w, r)
		}
	}
	return nil
}

func getMinioAccess() (*minio.Client, string, error) {
	p := promptui.Prompt{
		Label: "BucketName",
	}
	bucketName, err := p.Run()
	if err != nil {
		return nil, "", err
	}

	minioClient, err := getMinioClient()
	if err != nil {
		fmt.Println("cannot get minio client", err.Error())
		return nil, "", err
	}
	exists, err := minioClient.BucketExists(context.Background(), bucketName)
	if !exists {
		fmt.Printf("bucket %s not exists\n", bucketName)
		return nil, "", err
	}

	if !exists {
		fmt.Printf("Bucket not exist\n")
		return nil, "", errors.New("bucket not exists")
	}

	return minioClient, bucketName, nil
}

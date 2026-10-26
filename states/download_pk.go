package states

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/gosuri/uilive"
	"github.com/manifoldco/promptui"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type DownloadPKParam struct {
	OSSOverrideParam
	framework.ParamBase `use:"download-pk" desc:"download segment pk with provided collection/segment id"`
	MinioAddress        string `name:"minioAddr" default:"" desc:"the minio address to override, leave empty to use milvus.yaml value"`
	CollectionID        int64  `name:"collection" default:"0" desc:"collection id to download"`
	SegmentID           int64  `name:"segment" default:"0" desc:"segment id to download"`
}

func (s *InstanceState) DownloadPKCommand(ctx context.Context, p *DownloadPKParam) error {
	collection, err := common.GetCollectionByIDVersion(ctx, s.client, s.basePath, p.CollectionID)
	if err != nil {
		return err
	}
	pkField, ok := collection.GetPKField()
	if !ok {
		return errors.New("pk field not found")
	}

	segments, err := common.ListSegments(ctx, s.client, s.basePath, func(s *models.Segment) bool {
		return s.CollectionID == p.CollectionID && (p.SegmentID == 0 || p.SegmentID == s.ID)
	})
	if err != nil {
		return err
	}

	resolvedStore, err := s.getObjectStoreWithOverrides(ctx, p.OSSOverrideParam, p.MinioAddress, false)
	if err != nil {
		return err
	}

	return s.downloadPKs(ctx, resolvedStore.Store, resolvedStore.RootPath, p.CollectionID, pkField.FieldID, segments)
}

func (s *InstanceState) downloadPKs(ctx context.Context, store oss.ObjectStore, rootPath string, collID int64, pkID int64, segments []*models.Segment) error {
	folder := fmt.Sprintf("dlpks_%s", time.Now().Format("20060102150406"))
	err := os.Mkdir(folder, 0o777)
	if err != nil {
		fmt.Println("Failed to create folder,", err.Error())
	}

	pd := uilive.New()
	pf := "Downloading pk files ... %d%%(%d/%d)\n"
	pd.Start()
	fmt.Fprintf(pd, pf, 0, 0, len(segments))
	defer pd.Stop()

	count := 0
	for i, segment := range segments {
		targetFolder := fmt.Sprintf("%s/%d", folder, segment.ID)
		os.Mkdir(targetFolder, 0o777)
		for _, fieldBinlog := range segment.GetBinlogs() {
			if fieldBinlog.FieldID != pkID {
				continue
			}

			for _, binlog := range fieldBinlog.Binlogs {
				logPath := oss.ResolveObjectKey(rootPath, binlog.LogPath)
				obj, err := store.Open(ctx, logPath)
				if err != nil {
					fmt.Println("failed to download file", logPath)
					return err
				}

				name := path.Base(logPath)

				f, err := os.Create(path.Join(targetFolder, name))
				if err != nil {
					fmt.Println("failed to open file")
					return err
				}
				w := bufio.NewWriter(f)
				r := bufio.NewReader(obj)
				_, err = io.Copy(w, r)
				if err != nil {
					fmt.Println(err.Error())
				}
				w.Flush()
				f.Close()
				count++
			}
		}
		progress := (i + 1) * 100 / len(segments)
		fmt.Fprintf(pd, pf, progress, i+1, len(segments))
	}

	fmt.Println()
	fmt.Printf("pk file download completed for collection :%d, %d file(s) downloaded\n", collID, count)
	return nil
}

func getMinioClient() (*minio.Client, error) {
	p := promptui.Prompt{Label: "Address"}
	address, err := p.Run()
	if err != nil {
		return nil, err
	}

	ssl := promptui.Select{
		Label: "Use SSL",
		Items: []string{"yes", "no"},
	}
	_, sslResult, err := ssl.Run()
	if err != nil {
		return nil, err
	}
	useSSL := false
	switch sslResult {
	case "yes":
		useSSL = true
	case "no":
		useSSL = false
	}

	sl := promptui.Select{
		Label: "Select authentication method:",
		Items: []string{"IAM", "AK/SK"},
	}
	_, result, err := sl.Run()
	if err != nil {
		return nil, err
	}
	fmt.Println("Use authen: ", result)

	var cred *credentials.Credentials
	switch result {
	case "IAM":
		input := promptui.Prompt{
			Label: "IAM Endpoint",
		}

		iamEndpoint, err := input.Run()
		if err != nil {
			return nil, err
		}
		cred = credentials.NewIAM(iamEndpoint)
	case "AK/SK":
		p.HideEntered = true
		p.Mask = rune('*')
		p.Label = "AK"
		ak, err := p.Run()
		if err != nil {
			return nil, err
		}
		p.Label = "SK"
		sk, err := p.Run()
		if err != nil {
			return nil, err
		}

		cred = credentials.NewStaticV4(ak, sk, "")
	}

	minioClient, err := minio.New(address, &minio.Options{
		Creds:  cred,
		Secure: useSSL,
	})
	if err != nil {
		return nil, err
	}

	return minioClient, nil
}

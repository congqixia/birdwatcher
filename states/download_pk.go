package states

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/gosuri/uilive"
	"github.com/manifoldco/promptui"
	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func getDownloadPKCmd(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "download-pk",
		Short: "download pk column of a collection",
		RunE: func(cmd *cobra.Command, args []string) error {
			collectionID, err := cmd.Flags().GetInt64("id")
			if err != nil {
				return err
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			coll, err := common.GetCollectionByIDVersion(ctx, cli, basePath, etcdversion.GetVersion(), collectionID)
			if err != nil {
				fmt.Println("Collection not found for id", collectionID)
				return nil
			}

			var pkID int64 = -1

			for _, field := range coll.Schema.Fields {
				if field.IsPrimaryKey {
					pkID = field.FieldID
					break
				}
			}

			if pkID < 0 {
				fmt.Println("collection pk not found")
				return nil
			}

			segments, err := common.ListSegments(cli, basePath, func(segment *datapb.SegmentInfo) bool {
				return segment.CollectionID == collectionID
			})

			if err != nil {
				return err
			}

			p := promptui.Prompt{
				Label: "BucketName",
			}
			bucketName, err := p.Run()
			if err != nil {
				return err
			}

			minioClient, err := getMinioClient()
			if err != nil {
				fmt.Println("cannot get minio client", err.Error())
				return nil
			}
			exists, err := minioClient.BucketExists(context.Background(), bucketName)
			if !exists {
				fmt.Printf("bucket %s not exists\n", bucketName)
				return nil
			}

			if !exists {
				fmt.Printf("Bucket not exist\n")
				return nil
			}

			for _, segment := range segments {
				common.FillFieldsIfV2(cli, basePath, segment)
			}
			downloadPks(minioClient, bucketName, collectionID, pkID, segments)

			return nil
		},
	}

	cmd.Flags().Int64("id", 0, "collection id to download")
	return cmd
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

	cloudProvider := promptui.Select{
		Label: "Select Cloud provider",
		Items: []string{"aws", "gcp"},
	}
	_, cloudProviderResult, err := cloudProvider.Run()
	if err != nil {
		return nil, err
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

	opts := &minio.Options{
		Secure: useSSL,
	}
	// var cred *credentials.Credentials
	switch result {
	case "IAM":
		switch cloudProviderResult {
		case "aws":
			input := promptui.Prompt{
				Label: "IAM Endpoint",
			}

			iamEndpoint, err := input.Run()
			if err != nil {
				return nil, err
			}
			opts.Creds = credentials.NewIAM(iamEndpoint)
		case "gcp":
			transport, _ := NewWrapHTTPTransport(opts.Secure)
			opts.Creds = credentials.NewStaticV2("", "", "")
			opts.Transport = transport
		}
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

		switch cloudProviderResult {
		case "aws":
			opts.Creds = credentials.NewStaticV4(ak, sk, "")
		case "gcp":
			opts.Creds = credentials.NewStaticV2(ak, sk, "")
		}
	}

	minioClient, err := minio.New(address, opts)

	if err != nil {
		return nil, err
	}

	return minioClient, nil
}

func downloadPks(cli *minio.Client, bucketName string, collID, pkID int64, segments []*datapb.SegmentInfo) {
	err := os.Mkdir(fmt.Sprintf("%d", collID), 0777)
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
		for _, fieldBinlog := range segment.Binlogs {
			if fieldBinlog.FieldID != pkID {
				continue
			}

			folder := fmt.Sprintf("%d/%d", collID, segment.ID)
			err := os.MkdirAll(folder, 0777)
			if err != nil {
				fmt.Println("Failed to create sub-folder", err.Error())
				return
			}

			for _, binlog := range fieldBinlog.Binlogs {
				obj, err := cli.GetObject(context.Background(), bucketName, binlog.GetLogPath(), minio.GetObjectOptions{})
				if err != nil {
					fmt.Println("failed to download file", bucketName, binlog.GetLogPath())
					return
				}

				name := path.Base(binlog.GetLogPath())

				f, err := os.Create(path.Join(folder, name))
				if err != nil {
					fmt.Println("failed to open file")
					return
				}
				w := bufio.NewWriter(f)
				r := bufio.NewReader(obj)
				io.Copy(w, r)
				count++
			}
		}
		progress := (i + 1) * 100 / len(segments)
		fmt.Fprintf(pd, pf, progress, i+1, len(segments))

	}
	fmt.Println()
	fmt.Printf("pk file download completed for collection :%d, %d file(s) downloaded\n", collID, count)

}

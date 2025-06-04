package oss

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	CloudProviderGCP     = "gcp"
	CloudProviderAWS     = "aws"
	CloudProviderAliyun  = "aliyun"
	CloudProviderAzure   = "azure"
	CloudProviderTencent = "tencent"
)

type MinioClientParam struct {
	Addr          string
	Port          string
	AK            string
	SK            string
	UseIAM        bool
	IAMEndpoint   string
	UseSSL        bool
	CloudProvider string
	Region        string

	BucketName string
	RootPath   string

	skipCheckBucket bool
}

// MinioConnectParam is the function type to override client params
type MinioConnectParam func(p *MinioClientParam)

func WithSkipCheckBucket(v bool) MinioConnectParam {
	return func(p *MinioClientParam) {
		p.skipCheckBucket = v
	}
}

func WithMinioAddr(addr string) MinioConnectParam {
	return func(p *MinioClientParam) {
		p.Addr = addr
	}
}

func WithAK(ak string) MinioConnectParam {
	return func(p *MinioClientParam) {
		p.AK = ak
	}
}

func WithSK(sk string) MinioConnectParam {
	return func(p *MinioClientParam) {
		p.SK = sk
	}
}

// MinioClient wraps minio client, bucket info within
type MinioClient struct {
	Client     *minio.Client
	BucketName string
	RootPath   string
}

func NewMinioClient(ctx context.Context, p MinioClientParam) (*MinioClient, error) {
	opts := &minio.Options{
		Secure:       p.UseSSL,
		BucketLookup: minio.BucketLookupAuto,
	}
	endpoint := fmt.Sprintf("%s:%s", p.Addr, p.Port)

	var err error
	switch p.CloudProvider {
	case CloudProviderAWS:
		processMinioAwsOptions(p, opts)
	case CloudProviderGCP:
		// adhoc to remove port of gcs address to let minio-go know it's gcs
		if strings.Contains(endpoint, GcsDefaultAddress) {
			endpoint = GcsDefaultAddress
		}
		processMinioGcpOptions(p, opts)
	case CloudProviderAliyun:
		processMinioAliyunOptions(p, opts)
	case CloudProviderTencent:
		err = processMinioTencentOptions(p, opts)
	case CloudProviderAzure:
		// TODO support azure
		fallthrough
	default:
		return nil, errors.Newf("Cloud provider %s not supported yet", p.CloudProvider)
	}
	if err != nil {
		return nil, err
	}

	fmt.Printf("Start to connect to oss endpoind: %s\n", endpoint)
	client, err := minio.New(endpoint, opts)
	if err != nil {
		fmt.Println("new client failed: ", err.Error())
		return nil, err
	}

	fmt.Println("Connection successful!")

	if p.skipCheckBucket {
		fmt.Println("Skip bucket existence check...")
	} else {
		ok, err := client.BucketExists(ctx, p.BucketName)
		if err != nil {
			fmt.Printf("check bucket %s exists failed: %s\n", p.BucketName, err.Error())
			return nil, err
		}
		if !ok {
			return nil, errors.Newf("Bucket %s not exists", p.BucketName)
		}
	}

	return &MinioClient{
		Client:     client,
		BucketName: p.BucketName,
		RootPath:   p.RootPath,
	}, nil
}

func processMinioAwsOptions(p MinioClientParam, opts *minio.Options) {
	if p.UseIAM {
		opts.Creds = credentials.NewIAM(p.IAMEndpoint)
	} else {
		opts.Creds = credentials.NewStaticV4(p.AK, p.SK, "")
	}
}

package states

import (
	"context"
	"fmt"
	"strconv"

	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/ossutil"
)

// OSSOverrideParam uses strings for optional booleans so omitted flags preserve
// discovered configuration, while an explicit false overrides a true value.
// The existing minioAddr/skipBucketCheck flags remain on the owning command.
type OSSOverrideParam struct {
	LoadFrequency      string `name:"loadFrequency" default:"" desc:"override internal OSS credential reload interval (seconds)"`
	OSSStandalone      bool   `name:"ossStandalone" default:"false" desc:"build internal OSS client from flags without fetching component configuration"`
	Port               string `name:"port" default:"" desc:"override internal OSS port"`
	AccessKeyID        string `name:"accessKeyID" default:"" desc:"override internal OSS access key ID"`
	SecretAccessKey    string `name:"secretAccessKey" default:"" desc:"override internal OSS secret access key"`
	UseIAM             string `name:"useIAM" default:"" desc:"override internal OSS IAM: true or false (omitted inherits config)"`
	UseSSL             string `name:"useSSL" default:"" desc:"override internal OSS TLS: true or false (omitted inherits config)"`
	CloudProvider      string `name:"cloudProvider" default:"" desc:"override internal OSS provider: aws, gcp, aliyun, tencent, huawei"`
	Region             string `name:"region" default:"" desc:"override internal OSS region"`
	BucketName         string `name:"bucketName" default:"" desc:"override internal OSS bucket name"`
	RootPath           string `name:"rootPath" default:"" desc:"override internal OSS root path"`
	IAMEndpoint        string `name:"iamEndpoint" default:"" desc:"override internal OSS IAM endpoint"`
	RoleARN            string `name:"roleARN" default:"" desc:"override internal OSS role ARN"`
	RoleSessionName    string `name:"roleSessionName" default:"" desc:"override internal OSS role session name"`
	ExternalID         string `name:"externalID" default:"" desc:"override internal OSS role external ID"`
	AliyunRoleAuthMode string `name:"aliyunRoleAuthMode" default:"" desc:"override internal OSS Aliyun role auth mode: ram or oidc"`
}

func (p OSSOverrideParam) connectParams() ([]oss.MinioConnectParam, error) {
	var params []oss.MinioConnectParam
	if p.LoadFrequency != "" {
		value, err := strconv.Atoi(p.LoadFrequency)
		if err != nil || value < 0 {
			return nil, fmt.Errorf("--loadFrequency must be a non-negative integer")
		}
		params = append(params, oss.WithLoadFrequency(value))
	}
	for _, option := range []struct {
		name, value string
		apply       func(bool) oss.MinioConnectParam
	}{
		{"useIAM", p.UseIAM, oss.WithUseIAM}, {"useSSL", p.UseSSL, oss.WithUseSSL},
	} {
		if option.value == "" {
			continue
		}
		value, err := strconv.ParseBool(option.value)
		if err != nil {
			return nil, fmt.Errorf("--%s must be true or false", option.name)
		}
		params = append(params, option.apply(value))
	}
	for _, option := range []struct {
		value string
		apply func(string) oss.MinioConnectParam
	}{
		{p.Port, oss.WithPort}, {p.CloudProvider, oss.WithCloudProvider}, {p.Region, oss.WithRegion},
		{p.BucketName, oss.WithBucketName}, {p.RootPath, oss.WithRootPath}, {p.IAMEndpoint, oss.WithIAMEndpoint},
		{p.RoleARN, oss.WithRoleARN}, {p.RoleSessionName, oss.WithRoleSessionName}, {p.ExternalID, oss.WithExternalID},
		{p.AliyunRoleAuthMode, oss.WithAliyunRoleAuthMode},
	} {
		if option.value != "" {
			params = append(params, option.apply(option.value))
		}
	}
	if p.AccessKeyID != "" || p.SecretAccessKey != "" {
		// Override each independently so a single supplied credential does not clear
		// the other value from the discovered configuration.
		params = append(params, func(mp *oss.MinioClientParam) {
			if p.AccessKeyID != "" {
				mp.AK = p.AccessKeyID
			}
			if p.SecretAccessKey != "" {
				mp.SK = p.SecretAccessKey
			}
		})
	}
	return params, nil
}

func (s *InstanceState) getObjectStoreWithOverrides(ctx context.Context, p OSSOverrideParam, address string, skipBucketCheck bool) (*oss.ResolvedObjectStore, error) {
	params, err := p.connectParams()
	if err != nil {
		return nil, err
	}
	params = append(params, oss.WithSkipCheckBucket(skipBucketCheck))
	if address != "" {
		params = append(params, oss.WithMinioAddr(address))
	}
	if !p.OSSStandalone {
		return s.GetObjectStore(ctx, params...)
	}
	if address == "" || p.BucketName == "" || p.CloudProvider == "" {
		return nil, fmt.Errorf("--ossStandalone requires --minioAddr, --bucketName and --cloudProvider")
	}
	return ossutil.NewResolvedObjectStore(ctx, oss.MinioClientParam{}, params...)
}

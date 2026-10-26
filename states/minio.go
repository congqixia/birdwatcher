package states

import (
	"context"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/manifoldco/promptui"
	"github.com/minio/minio-go/v7"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/oss"
)

type TestMinioCfgParam struct {
	framework.ParamBase `use:"test-minio-cfg"`
	// *MinioConnectParam
	OSSOverrideParam
	MinioAddress  string `name:"minioAddress"`
	MinioPasswd   string `name:"minioPassword"`
	MinioUserName string `name:"minioUsername"`
}

type MinioConnectParam struct {
	MinioAddress  string `name:"minioAddress"`
	MinioPasswd   string `name:"minioPassword"`
	MinioUserName string `name:"minioUsername"`
}

func (s *InstanceState) TestMinioCfgCommand(ctx context.Context, p *TestMinioCfgParam) error {
	resolved, err := s.getObjectStoreWithOverrides(ctx, p.OSSOverrideParam, p.MinioAddress, false)
	if err != nil {
		return err
	}
	if _, ok := oss.MinioClientFromObjectStore(resolved.Store); !ok {
		return errors.New("resolved object store is not backed by minio client")
	}
	return nil
}

func (s *InstanceState) GetMinioClientFromCfg(ctx context.Context, params ...oss.MinioConnectParam) (client *minio.Client, bucketName, rootPath string, err error) {
	resolved, err := s.GetObjectStore(ctx, params...)
	if err != nil {
		return nil, "", "", err
	}
	client, ok := oss.MinioClientFromObjectStore(resolved.Store)
	if !ok {
		return nil, "", "", errors.New("resolved object store is not backed by minio client")
	}
	return client, resolved.BucketName, resolved.RootPath, nil
}

func (s *InstanceState) GetMinioClientFromPrompt(ctx context.Context) (client *minio.Client, bucketName, rootPath string, err error) {
	p := promptui.Prompt{
		Label: "BucketName",
	}
	bucketName, err = p.Run()
	if err != nil {
		return nil, "", "", err
	}

	p = promptui.Prompt{
		Label: "Root Path",
	}
	rootPath, err = p.Run()
	if err != nil {
		return nil, "", "", err
	}

	p = promptui.Prompt{Label: "Address"}
	address, err := p.Run()
	if err != nil {
		return nil, "", "", err
	}

	ssl := promptui.Select{
		Label: "Use SSL",
		Items: []string{"yes", "no"},
	}
	_, sslResult, err := ssl.Run()
	if err != nil {
		return nil, "", "", err
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
		Items: []string{"aws", "aliyun", "gcp"},
	}
	_, cloudProviderResult, err := cloudProvider.Run()
	if err != nil {
		return nil, "", "", err
	}

	sl := promptui.Select{
		Label: "Select authentication method:",
		Items: []string{"RoleARN", "IAM", "AK/SK"},
	}
	_, result, err := sl.Run()
	if err != nil {
		return nil, "", "", err
	}

	mp := oss.MinioClientParam{
		CloudProvider: cloudProviderResult,
		Addr:          address,
		UseSSL:        useSSL,
		BucketName:    bucketName,
		RootPath:      rootPath,
	}

	switch result {
	case "RoleARN":
		input := promptui.Prompt{Label: "Role ARN"}
		roleARN, err := input.Run()
		if err != nil {
			return nil, "", "", err
		}
		mp.RoleARN = roleARN

		input = promptui.Prompt{Label: "Role Session Name (optional)"}
		roleSessionName, err := input.Run()
		if err != nil {
			return nil, "", "", err
		}
		mp.RoleSessionName = roleSessionName

		input = promptui.Prompt{Label: "External ID (optional)"}
		externalID, err := input.Run()
		if err != nil {
			return nil, "", "", err
		}
		mp.ExternalID = externalID

		input = promptui.Prompt{Label: "Load Frequency Seconds (optional)"}
		loadFrequency, err := input.Run()
		if err != nil {
			return nil, "", "", err
		}
		if loadFrequency != "" {
			value, err := strconv.Atoi(loadFrequency)
			if err != nil {
				return nil, "", "", errors.Wrapf(err, "invalid load frequency: %s", loadFrequency)
			}
			mp.LoadFrequency = value
		}

		if cloudProviderResult == oss.CloudProviderAliyun {
			mode := promptui.Select{
				Label: "Aliyun Role Auth Mode",
				Items: []string{"ram", "oidc"},
			}
			_, aliyunRoleAuthMode, err := mode.Run()
			if err != nil {
				return nil, "", "", err
			}
			mp.AliyunRoleAuthMode = aliyunRoleAuthMode
		}

		if cloudProviderResult == oss.CloudProviderAWS {
			mode := promptui.Select{
				Label: "Base Credential Source",
				Items: []string{"DefaultChain", "AK/SK"},
			}
			_, baseSource, err := mode.Run()
			if err != nil {
				return nil, "", "", err
			}
			if baseSource == "AK/SK" {
				p.HideEntered = true
				p.Mask = rune('*')
				p.Label = "AK"
				ak, err := p.Run()
				if err != nil {
					return nil, "", "", err
				}
				p.Label = "SK"
				sk, err := p.Run()
				if err != nil {
					return nil, "", "", err
				}
				mp.AK = ak
				mp.SK = sk
			}
		}
	case "IAM":
		mp.UseIAM = true
		input := promptui.Prompt{
			Label: "IAM Endpoint",
		}
		iamEndpoint, err := input.Run()
		if err != nil {
			return nil, "", "", err
		}

		mp.IAMEndpoint = iamEndpoint
	case "AK/SK":
		p.HideEntered = true
		p.Mask = rune('*')
		p.Label = "AK"
		ak, err := p.Run()
		if err != nil {
			return nil, "", "", err
		}
		p.Label = "SK"
		sk, err := p.Run()
		if err != nil {
			return nil, "", "", err
		}

		mp.AK = ak
		mp.SK = sk
	}

	mClient, err := oss.NewMinioClient(ctx, mp)
	if err != nil {
		return nil, "", "", err
	}

	return mClient.Client, bucketName, rootPath, nil
}

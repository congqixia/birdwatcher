package states

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	binlogv1 "github.com/milvus-io/birdwatcher/storage/binlog/v1"
	storagecommon "github.com/milvus-io/birdwatcher/storage/common"
	storagemanifest "github.com/milvus-io/birdwatcher/storage/manifest"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
)

type closeTrackingReadSeeker struct {
	*bytes.Reader
	closed bool
}

func (r *closeTrackingReadSeeker) Close() error {
	r.closed = true
	return nil
}

type singleObjectStore struct {
	object storagecommon.ReadSeeker
}

func (s *singleObjectStore) Open(context.Context, string, ...oss.OpenOption) (storagecommon.ReadSeeker, error) {
	return s.object, nil
}

func (*singleObjectStore) Stat(context.Context, string) (*models.FsStat, error) {
	return nil, nil
}

func (*singleObjectStore) List(context.Context, string, bool) (<-chan oss.ObjectInfo, error) {
	return nil, nil
}

func TestValidateInspectParquetParam(t *testing.T) {
	tests := []struct {
		name    string
		param   InspectParquetParam
		wantErr bool
	}{
		{
			name:    "local file mode",
			param:   InspectParquetParam{FilePath: "/tmp/a.parquet"},
			wantErr: false,
		},
		{
			name:    "segment mode",
			param:   InspectParquetParam{SegmentID: 100},
			wantErr: false,
		},
		{
			name:    "segment auto storage version",
			param:   InspectParquetParam{SegmentID: 100, StorageVersion: "auto"},
			wantErr: false,
		},
		{
			name:    "segment storage version override",
			param:   InspectParquetParam{SegmentID: 100, StorageVersion: "2"},
			wantErr: false,
		},
		{
			name:    "external manifest mode",
			param:   InspectParquetParam{External: true, CollectionID: 1, ManifestSegmentID: 10},
			wantErr: false,
		},
		{
			name:    "external file mode",
			param:   InspectParquetParam{External: true, CollectionID: 1, ExternalFile: "a/b.parquet"},
			wantErr: false,
		},
		{
			name:    "missing selectors",
			param:   InspectParquetParam{},
			wantErr: true,
		},
		{
			name:    "conflicting local selectors",
			param:   InspectParquetParam{FilePath: "/tmp/a.parquet", SegmentID: 10},
			wantErr: true,
		},
		{
			name:    "local file rejects auto storage version",
			param:   InspectParquetParam{FilePath: "/tmp/a.parquet", StorageVersion: "auto"},
			wantErr: true,
		},
		{
			name:    "invalid storage version",
			param:   InspectParquetParam{SegmentID: 100, StorageVersion: "invalid"},
			wantErr: true,
		},
		{
			name:    "negative storage version",
			param:   InspectParquetParam{SegmentID: 100, StorageVersion: "-1"},
			wantErr: true,
		},
		{
			name:    "external missing collection",
			param:   InspectParquetParam{External: true, ManifestSegmentID: 10},
			wantErr: true,
		},
		{
			name:    "external missing target",
			param:   InspectParquetParam{External: true, CollectionID: 1},
			wantErr: true,
		},
		{
			name:    "external conflicting targets",
			param:   InspectParquetParam{External: true, CollectionID: 1, ManifestSegmentID: 10, ExternalFile: "a.parquet"},
			wantErr: true,
		},
		{
			name:    "external conflicts with segment",
			param:   InspectParquetParam{External: true, CollectionID: 1, ManifestSegmentID: 10, SegmentID: 20},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateInspectParquetParam(&tt.param)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateInspectParquetParam() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseInspectStorageVersionOption(t *testing.T) {
	tests := []struct {
		name        string
		value       string
		wantMode    inspectStorageVersionMode
		wantVersion int64
		wantErr     bool
	}{
		{name: "empty uses meta", value: "", wantMode: inspectStorageVersionFromMeta},
		{name: "meta", value: "meta", wantMode: inspectStorageVersionFromMeta},
		{name: "auto case insensitive", value: " AUTO ", wantMode: inspectStorageVersionAuto},
		{name: "v1 compatibility value", value: "1", wantMode: inspectStorageVersionOverride, wantVersion: 1},
		{name: "v2", value: "2", wantMode: inspectStorageVersionOverride, wantVersion: 2},
		{name: "future manifest version", value: "4", wantMode: inspectStorageVersionOverride, wantVersion: 4},
		{name: "negative", value: "-1", wantErr: true},
		{name: "invalid", value: "v2", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseInspectStorageVersionOption(tt.value)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseInspectStorageVersionOption() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if got.mode != tt.wantMode || got.version != tt.wantVersion {
				t.Fatalf("parseInspectStorageVersionOption() = %#v, want mode=%v version=%d", got, tt.wantMode, tt.wantVersion)
			}
		})
	}
}

func TestDetectBinlogStorageVersion(t *testing.T) {
	v1Header := make([]byte, 4)
	binary.LittleEndian.PutUint32(v1Header, uint32(binlogv1.MagicNumberV1))

	tests := []struct {
		name    string
		data    []byte
		want    int64
		wantErr bool
	}{
		{name: "v1 wrapper", data: append(v1Header, 0x01), want: 0},
		{name: "raw parquet", data: []byte("PAR1payload"), want: 2},
		{name: "unknown magic", data: []byte("NOPE"), wantErr: true},
		{name: "short header", data: []byte("PAR"), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bytes.NewReader(tt.data)
			got, err := detectBinlogStorageVersion(reader)
			if (err != nil) != tt.wantErr {
				t.Fatalf("detectBinlogStorageVersion() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Fatalf("detectBinlogStorageVersion() = %d, want %d", got, tt.want)
			}
			position, seekErr := reader.Seek(0, 1)
			if seekErr != nil {
				t.Fatalf("reader.Seek() error = %v", seekErr)
			}
			if position != 0 {
				t.Fatalf("detectBinlogStorageVersion() changed reader position to %d", position)
			}
		})
	}
}

func TestParseExternalSpec(t *testing.T) {
	raw := `{
		"format": "parquet",
		"extfs": {
			"cloud_provider": "aliyun",
			"external_id": "ext-123",
			"region": "cn-hangzhou",
			"role_arn": "acs:ram::1:role/demo",
			"session_name": "birdwatcher-test",
			"use_ssl": "true",
			"use_virtual_host": "true",
			"load_frequency": "3600"
		}
	}`

	spec, err := parseExternalSpec(raw)
	if err != nil {
		t.Fatalf("parseExternalSpec() error = %v", err)
	}
	if spec.Format != "parquet" {
		t.Fatalf("unexpected format: %s", spec.Format)
	}
	if spec.CloudProvider != "aliyun" {
		t.Fatalf("unexpected cloud provider: %s", spec.CloudProvider)
	}
	if spec.Region != "cn-hangzhou" {
		t.Fatalf("unexpected region: %s", spec.Region)
	}
	if spec.RoleARN != "acs:ram::1:role/demo" {
		t.Fatalf("unexpected role arn: %s", spec.RoleARN)
	}
	if spec.ExternalID != "ext-123" {
		t.Fatalf("unexpected external id: %s", spec.ExternalID)
	}
	if spec.RoleSessionName != "birdwatcher-test" {
		t.Fatalf("unexpected role session name: %s", spec.RoleSessionName)
	}
	if spec.UseSSL == nil || !*spec.UseSSL {
		t.Fatalf("expected use_ssl=true, got %#v", spec.UseSSL)
	}
	if spec.UseVirtualHost == nil || !*spec.UseVirtualHost {
		t.Fatal("expected use_virtual_host=true")
	}
	if spec.LoadFrequency != 3600 {
		t.Fatalf("unexpected load frequency: %d", spec.LoadFrequency)
	}
}

func TestParseExternalSpecPreservesAliyunRoleAuthMode(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{
			name: "top level",
			raw: `{
				"format":"parquet",
				"aliyun_role_auth_mode":"ram",
				"extfs":{
					"cloud_provider":"aliyun",
					"region":"cn-hangzhou",
					"role_arn":"acs:ram::1:role/demo",
					"aliyun_role_auth_mode":"oidc"
				}
			}`,
		},
		{
			name: "legacy extfs extension",
			raw: `{
				"format":"parquet",
				"extfs":{
					"cloud_provider":"aliyun",
					"region":"cn-hangzhou",
					"role_arn":"acs:ram::1:role/demo",
					"aliyun_role_auth_mode":"ram"
				}
			}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec, err := parseExternalSpec(test.raw)
			if err != nil {
				t.Fatalf("parseExternalSpec() error = %v", err)
			}
			if spec.AliyunRoleAuthMode != "ram" {
				t.Fatalf("AliyunRoleAuthMode = %q, want ram", spec.AliyunRoleAuthMode)
			}
			if _, ok := spec.Extfs["aliyun_role_auth_mode"]; ok {
				t.Fatal("legacy extension must not be forwarded to canonical extfs validation")
			}

			param, _, err := buildExternalMinioClientParam(
				"s3://oss-cn-hangzhou.aliyuncs.com/test-bucket/testlake/parquet",
				spec,
				true,
			)
			if err != nil {
				t.Fatalf("buildExternalMinioClientParam() error = %v", err)
			}
			if param.AliyunRoleAuthMode != "ram" {
				t.Fatalf("AliyunRoleAuthMode = %q, want ram", param.AliyunRoleAuthMode)
			}
		})
	}
}

func TestBuildExternalMinioClientParamDefaultsAliyunRoleMode(t *testing.T) {
	spec, err := parseExternalSpec(`{
		"format":"parquet",
		"extfs":{
			"cloud_provider":"aliyun",
			"region":"cn-hangzhou",
			"role_arn":"acs:ram::1:role/demo"
		}
	}`)
	if err != nil {
		t.Fatalf("parseExternalSpec() error = %v", err)
	}
	param, _, err := buildExternalMinioClientParam(
		"s3://oss-cn-hangzhou.aliyuncs.com/test-bucket/testlake/parquet",
		spec,
		true,
	)
	if err != nil {
		t.Fatalf("buildExternalMinioClientParam() error = %v", err)
	}
	if param.AliyunRoleAuthMode != "oidc" {
		t.Fatalf("AliyunRoleAuthMode = %q, want oidc", param.AliyunRoleAuthMode)
	}
}

func TestParseExternalSource(t *testing.T) {
	tests := []struct {
		name         string
		source       string
		spec         externalSourceSpec
		wantHost     string
		wantBucket   string
		wantRoot     string
		hostIsBucket bool
	}{
		{
			name:       "milvus form cloud endpoint",
			source:     "oss://oss-cn-hangzhou.aliyuncs.com/test-oss-0815/testlake/parquet",
			spec:       externalSourceSpec{CloudProvider: "aliyun", Region: "cn-hangzhou"},
			wantHost:   "oss-cn-hangzhou.aliyuncs.com",
			wantBucket: "test-oss-0815",
			wantRoot:   "testlake/parquet",
		},
		{
			name:         "aws form",
			source:       "s3://customer-bucket/testlake/parquet",
			spec:         externalSourceSpec{CloudProvider: "aws", Region: "eu-west-2"},
			wantHost:     "s3.eu-west-2.amazonaws.com",
			wantBucket:   "customer-bucket",
			wantRoot:     "testlake/parquet",
			hostIsBucket: true,
		},
		{
			name:       "minio form",
			source:     "minio://localhost:9000/test-bucket/testlake/parquet",
			spec:       externalSourceSpec{CloudProvider: "minio"},
			wantHost:   "localhost:9000",
			wantBucket: "test-bucket",
			wantRoot:   "testlake/parquet",
		},
		{
			name:       "s3 scheme with minio provider",
			source:     "s3://custom.example:9000/test-bucket/testlake/parquet",
			spec:       externalSourceSpec{CloudProvider: "minio"},
			wantHost:   "custom.example:9000",
			wantBucket: "test-bucket",
			wantRoot:   "testlake/parquet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			location, err := parseExternalSource(tt.source, tt.spec)
			if err != nil {
				t.Fatalf("parseExternalSource() error = %v", err)
			}
			if location.Host != tt.wantHost || location.Bucket != tt.wantBucket ||
				location.RootPath != tt.wantRoot || location.HostIsBucket != tt.hostIsBucket {
				t.Fatalf("parseExternalSource() = %#v", location)
			}
		})
	}
}

func TestResolveExternalObjectKey(t *testing.T) {
	location := externalSourceLocation{
		SourceHost: "oss-cn-hangzhou.aliyuncs.com",
		Host:       "oss-cn-hangzhou.aliyuncs.com",
		Bucket:     "test-oss-0815",
		RootPath:   "testlake/parquet",
	}

	tests := []struct {
		name string
		file string
		want string
	}{
		{
			name: "relative path",
			file: "part-0001.parquet",
			want: "testlake/parquet/part-0001.parquet",
		},
		{
			name: "already rooted",
			file: "testlake/parquet/part-0002.parquet",
			want: "testlake/parquet/part-0002.parquet",
		},
		{
			name: "full uri",
			file: "oss://oss-cn-hangzhou.aliyuncs.com/test-oss-0815/testlake/parquet/part-0003.parquet",
			want: "testlake/parquet/part-0003.parquet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveExternalObjectKey(location, tt.file)
			if err != nil {
				t.Fatalf("resolveExternalObjectKey() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("resolveExternalObjectKey() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestResolveExternalObjectKey_AWSForm(t *testing.T) {
	location, err := parseExternalSource(
		"s3://customer-bucket/testlake/parquet",
		externalSourceSpec{CloudProvider: "aws", Region: "eu-west-2"},
	)
	if err != nil {
		t.Fatalf("parseExternalSource() error = %v", err)
	}

	got, err := resolveExternalObjectKey(
		location,
		"s3://customer-bucket/testlake/parquet/part-0001.parquet",
	)
	if err != nil {
		t.Fatalf("resolveExternalObjectKey() error = %v", err)
	}
	if got != "testlake/parquet/part-0001.parquet" {
		t.Fatalf("resolveExternalObjectKey() = %s", got)
	}
}

func TestBuildExternalMinioClientParam(t *testing.T) {
	spec, err := parseExternalSpec(`{
		"format":"parquet",
		"extfs":{
			"cloud_provider":"aws",
			"region":"eu-west-2",
			"access_key_id":"test-ak",
			"access_key_value":"test-sk",
			"iam_endpoint":"http://169.254.169.254",
			"use_ssl":"true",
			"use_virtual_host":"true"
		}
	}`)
	if err != nil {
		t.Fatalf("parseExternalSpec() error = %v", err)
	}

	param, location, err := buildExternalMinioClientParam(
		"s3://customer-bucket/testlake/parquet",
		spec,
		true,
	)
	if err != nil {
		t.Fatalf("buildExternalMinioClientParam() error = %v", err)
	}
	if param.Addr != "s3.eu-west-2.amazonaws.com" ||
		param.BucketName != "customer-bucket" ||
		param.RootPath != "testlake/parquet" {
		t.Fatalf("unexpected storage location: location=%#v", location)
	}
	if param.AK != "test-ak" || param.SK != "test-sk" || !param.UseSSL {
		t.Fatal("static credentials or SSL settings were not propagated")
	}
	if param.UseVirtualHost == nil || !*param.UseVirtualHost {
		t.Fatal("use_virtual_host was not propagated")
	}
}

func TestBuildExternalMinioClientParamAzureBroker(t *testing.T) {
	spec, err := parseExternalSpec(`{
		"format":"parquet",
		"extfs":{
			"cloud_provider":"azure",
			"region":"westus3",
			"access_key_id":"storage-account",
			"azure_client_id":"client-ID",
			"azure_tenant_id":"tenant-ID",
			"azure_credential_endpoint":"https://broker.example.com/v1/Credentials",
			"load_frequency":"3600"
		}
	}`)
	if err != nil {
		t.Fatalf("parseExternalSpec() error = %v", err)
	}
	if spec.AzureClientID != "client-ID" || spec.AzureTenantID != "tenant-ID" ||
		spec.AzureCredentialEndpoint != "https://broker.example.com/v1/Credentials" {
		t.Fatalf("Azure broker spec = %#v", spec)
	}

	param, location, err := buildExternalMinioClientParam(
		"azure://core.windows.net/container/root/path",
		spec,
		true,
	)
	if err != nil {
		t.Fatalf("buildExternalMinioClientParam() error = %v", err)
	}
	if param.CloudProvider != "azure" || param.Addr != "core.windows.net" ||
		param.BucketName != "container" || param.RootPath != "root/path" ||
		param.AK != "storage-account" || param.AzureClientID != "client-ID" ||
		param.AzureTenantID != "tenant-ID" ||
		param.AzureCredentialEndpoint != "https://broker.example.com/v1/Credentials" ||
		!param.DisableAzureConnectionString {
		t.Fatalf("Azure client parameters = %#v, location = %#v", param, location)
	}
	store, _, _, _, err := newExternalObjectStore(
		t.Context(),
		"azure://core.windows.net/container/root/path",
		spec,
		true,
	)
	if err != nil {
		t.Fatalf("newExternalObjectStore() error = %v", err)
	}
	if store == nil {
		t.Fatal("newExternalObjectStore() returned nil store")
	}
}

func TestBuildExternalMinioClientParam_LegacyFallback(t *testing.T) {
	spec, err := parseExternalSpec("")
	if err != nil {
		t.Fatalf("parseExternalSpec() error = %v", err)
	}

	param, location, err := buildExternalMinioClientParam(
		"s3://legacy-endpoint/legacy-bucket/root/path",
		spec,
		true,
	)
	if err != nil {
		t.Fatalf("buildExternalMinioClientParam() error = %v", err)
	}
	if location.Host != "legacy-endpoint" || location.Bucket != "legacy-bucket" ||
		location.RootPath != "root/path" {
		t.Fatalf("unexpected legacy storage location: %#v", location)
	}
	if param.CloudProvider != "aws" || !param.UseIAM || !param.UseSSL {
		t.Fatalf("unexpected legacy client parameters: %#v", param)
	}
}

func TestBuildExternalMinioClientParam_EmptyExtfsLegacyFallback(t *testing.T) {
	spec, err := parseExternalSpec(`{"format":"parquet","extfs":{}}`)
	if err != nil {
		t.Fatalf("parseExternalSpec() error = %v", err)
	}

	param, location, err := buildExternalMinioClientParam(
		"s3://legacy-endpoint/legacy-bucket/root/path",
		spec,
		true,
	)
	if err != nil {
		t.Fatalf("buildExternalMinioClientParam() error = %v", err)
	}
	if location.Host != "legacy-endpoint" || location.Bucket != "legacy-bucket" ||
		location.RootPath != "root/path" {
		t.Fatalf("unexpected legacy storage location: %#v", location)
	}
	if param.CloudProvider != "aws" || !param.UseIAM || !param.UseSSL {
		t.Fatalf("unexpected legacy client parameters: %#v", param)
	}
}

func TestBuildExternalMinioClientParamRejectsIncompleteExtfs(t *testing.T) {
	spec, err := parseExternalSpec(`{
		"format":"parquet",
		"extfs":{
			"cloud_provider":"aws",
			"region":"eu-west-2",
			"access_key_id":"test-ak"
		}
	}`)
	if err != nil {
		t.Fatalf("parseExternalSpec() error = %v", err)
	}

	_, _, err = buildExternalMinioClientParam(
		"s3://customer-bucket/testlake/parquet",
		spec,
		true,
	)
	if err == nil || !strings.Contains(err.Error(), "must be set together") {
		t.Fatalf("buildExternalMinioClientParam() error = %v", err)
	}
}

func TestBuildExternalMinioClientParam_AuthAndMinioModes(t *testing.T) {
	t.Run("iam", func(t *testing.T) {
		spec, err := parseExternalSpec(`{
			"format":"parquet",
			"extfs":{
				"cloud_provider":"aws",
				"region":"eu-west-2",
				"use_iam":"true",
				"iam_endpoint":"http://169.254.169.254",
				"use_virtual_host":"false"
			}
		}`)
		if err != nil {
			t.Fatalf("parseExternalSpec() error = %v", err)
		}

		param, _, err := buildExternalMinioClientParam(
			"s3://customer-bucket/testlake/parquet",
			spec,
			true,
		)
		if err != nil {
			t.Fatalf("buildExternalMinioClientParam() error = %v", err)
		}
		if !param.UseIAM || param.IAMEndpoint != "http://169.254.169.254" {
			t.Fatal("IAM settings were not propagated")
		}
		if param.UseVirtualHost == nil || *param.UseVirtualHost {
			t.Fatal("path-style bucket lookup was not propagated")
		}
	})

	t.Run("minio", func(t *testing.T) {
		spec, err := parseExternalSpec(`{
			"format":"parquet",
			"extfs":{
				"cloud_provider":"minio",
				"access_key_id":"minio-ak",
				"access_key_value":"minio-sk"
			}
		}`)
		if err != nil {
			t.Fatalf("parseExternalSpec() error = %v", err)
		}

		param, _, err := buildExternalMinioClientParam(
			"minio://localhost:9000/test-bucket/testlake/parquet",
			spec,
			true,
		)
		if err != nil {
			t.Fatalf("buildExternalMinioClientParam() error = %v", err)
		}
		if param.CloudProvider != "minio" || param.Addr != "localhost:9000" ||
			param.BucketName != "test-bucket" || param.UseSSL {
			t.Fatal("MinIO source settings were not propagated")
		}
		if param.UseVirtualHost != nil {
			t.Fatal("unspecified use_virtual_host must preserve the provider default")
		}
	})

	t.Run("aliyun provider default", func(t *testing.T) {
		spec, err := parseExternalSpec(`{
			"format":"parquet",
			"extfs":{
				"cloud_provider":"aliyun",
				"region":"cn-hangzhou",
				"access_key_id":"aliyun-ak",
				"access_key_value":"aliyun-sk"
			}
		}`)
		if err != nil {
			t.Fatalf("parseExternalSpec() error = %v", err)
		}

		param, _, err := buildExternalMinioClientParam(
			"s3://oss-cn-hangzhou.aliyuncs.com/test-bucket/testlake/parquet",
			spec,
			true,
		)
		if err != nil {
			t.Fatalf("buildExternalMinioClientParam() error = %v", err)
		}
		if param.CloudProvider != "aliyun" || param.UseVirtualHost != nil {
			t.Fatal("unspecified use_virtual_host must preserve Aliyun DNS bucket lookup")
		}
	})

	t.Run("anonymous", func(t *testing.T) {
		spec, err := parseExternalSpec(`{
			"format":"parquet",
			"extfs":{
				"cloud_provider":"aws",
				"region":"eu-west-2",
				"anonymous":"true"
			}
		}`)
		if err != nil {
			t.Fatalf("parseExternalSpec() error = %v", err)
		}

		param, _, err := buildExternalMinioClientParam(
			"s3://public-bucket/testlake/parquet",
			spec,
			true,
		)
		if err != nil {
			t.Fatalf("buildExternalMinioClientParam() error = %v", err)
		}
		if !param.Anonymous || param.AK != "" || param.SK != "" {
			t.Fatal("anonymous credential mode was not propagated")
		}
	})

	t.Run("role arn", func(t *testing.T) {
		spec, err := parseExternalSpec(`{
			"format":"parquet",
			"extfs":{
				"cloud_provider":"aws",
				"region":"eu-west-2",
				"role_arn":"arn:aws:iam::123456789012:role/test",
				"session_name":"birdwatcher-test",
				"external_id":"external-test"
			}
		}`)
		if err != nil {
			t.Fatalf("parseExternalSpec() error = %v", err)
		}

		param, _, err := buildExternalMinioClientParam(
			"s3://customer-bucket/testlake/parquet",
			spec,
			true,
		)
		if err != nil {
			t.Fatalf("buildExternalMinioClientParam() error = %v", err)
		}
		if param.RoleARN == "" || param.RoleSessionName != "birdwatcher-test" ||
			param.ExternalID != "external-test" || param.UseIAM {
			t.Fatal("role ARN credential mode was not propagated independently from IAM")
		}
	})
}

func TestInspectExternalManifestParquetClosesManifestObject(t *testing.T) {
	object := &closeTrackingReadSeeker{Reader: bytes.NewReader([]byte("invalid manifest"))}
	store := &singleObjectStore{object: object}
	segment := &models.Segment{SegmentInfo: &datapb.SegmentInfo{
		ID:           10,
		ManifestPath: `{"ver":1,"base_path":"files"}`,
	}}

	err := inspectExternalManifestParquet(
		context.Background(),
		store,
		"root",
		nil,
		externalSourceLocation{},
		nil,
		segment,
		&InspectParquetParam{},
	)
	if err == nil {
		t.Fatal("inspectExternalManifestParquet() expected manifest parse error")
	}
	if !object.closed {
		t.Fatal("inspectExternalManifestParquet() did not close the manifest object")
	}
}

// TestInspectParquetExternalModes exercises the command with real manifest and
// Parquet bytes. Only the object-store factory is replaced.
func TestInspectParquetExternalModes(t *testing.T) {
	for _, format := range []string{"", "parquet", "iceberg-table", "lance-table", "unknown"} {
		for _, single := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/single=%t", format, single), func(t *testing.T) {
				s, internalStore, segment := newScanCommandFixture(t)
				client := s.client.(*scanFixtureKV)
				spec := fmt.Sprintf(`{"format":%q}`, format)
				data, err := proto.Marshal(&etcdpb.CollectionInfo{ID: 1, Schema: &schemapb.CollectionSchema{
					Name: "external", ExternalSource: "s3://warehouse/table/metadata/v18.json", ExternalSpec: spec,
				}})
				require.NoError(t, err)
				client.data["root/root-coord/collection/1"] = string(data)
				field, err := proto.Marshal(&schemapb.FieldSchema{FieldID: 100, Name: "id", ExternalField: "source_id"})
				require.NoError(t, err)
				client.data["root/root-coord/fields/1/100"] = string(field)
				// A source column may have the same name as a generated field.
				generated, err := proto.Marshal(&schemapb.FieldSchema{
					FieldID: 101, Name: "source_id", IsFunctionOutput: true,
				})
				require.NoError(t, err)
				client.data["root/root-coord/fields/1/101"] = string(generated)
				externalStore := &scanFixtureStore{data: map[string][]byte{
					"table/data/part.parquet": scanFixtureParquet(t, []string{"source_id"}, [][]int64{{1}, {2}, {3}}),
				}, opens: make(map[string]int), closes: make(map[string]int)}
				location := externalSourceLocation{
					Host: "storage.example", SourceHost: "warehouse", Bucket: "warehouse",
					RootPath: "table/metadata/v18.json", HostIsBucket: true,
				}
				uri := "s3://storage.example/warehouse/table/data/part.parquet"
				if format != "iceberg-table" {
					location.RootPath = "table"
					uri = "s3://warehouse/table/data/part.parquet"
				}
				patch := mockey.Mock(newExternalObjectStore).Return(externalStore, location.Bucket, location.RootPath, location, nil).Build()
				defer patch.UnPatch()
				internalStore.data["files/insert_log/1/2/20/_metadata/manifest-7.avro"] = scanFixtureManifest(t, &storagemanifest.Manifest{
					ColumnGroups: []storagemanifest.ColumnGroup{
						{Columns: []string{"source_id"}, Format: format, Files: []storagemanifest.ColumnGroupFile{{Path: uri, StartIndex: 1, EndIndex: 2}}},
						{Columns: []string{"101"}, Format: "parquet", Files: []storagemanifest.ColumnGroupFile{{Path: "generated.parquet", EndIndex: 1}}},
					},
				})
				internalStore.data["files/insert_log/1/2/20/_data/generated.parquet"] = scanFixtureParquet(t, []string{"101"}, [][]int64{{8}})
				p := &InspectParquetParam{External: true, CollectionID: 1, MetadataOnly: true, ShowRowGroups: true}
				if single {
					p.ExternalFile = uri
				} else {
					p.ManifestSegmentID = segment.ID
				}
				output, err := captureScanOutput(t, func() error { return s.InspectParquetCommand(t.Context(), p) })
				if format == "lance-table" || format == "unknown" {
					require.Error(t, err)
					require.Empty(t, externalStore.opens)
					return
				}
				require.NoError(t, err)
				require.Equal(t, 1, externalStore.opens["table/data/part.parquet"])
				require.Contains(t, output, "Physical NumRows: 3")
				require.Contains(t, output, "RowGroup 0:")
				if format == "iceberg-table" {
					require.Contains(t, output, "deletes and fragment row ranges are not applied")
				}
				if !single {
					require.Equal(t, 1, internalStore.opens["files/insert_log/1/2/20/_data/generated.parquet"])
					p.FieldID = 100
					require.NoError(t, s.InspectParquetCommand(t.Context(), p))
					require.Equal(t, 2, externalStore.opens["table/data/part.parquet"])
					require.Equal(t, 1, internalStore.opens["files/insert_log/1/2/20/_data/generated.parquet"])
				}
				if !single {
					p.FieldID = 101
					require.NoError(t, s.InspectParquetCommand(t.Context(), p))
					require.Equal(t, 2, externalStore.opens["table/data/part.parquet"], "generated field must not select the same-named source column")
					require.Equal(t, 2, internalStore.opens["files/insert_log/1/2/20/_data/generated.parquet"])
					p.FieldID = 999
					require.Error(t, s.InspectParquetCommand(t.Context(), p))
					p.FieldID = 0
				}
				if single && format == "iceberg-table" {
					for _, invalid := range []string{"table/data/part.parquet", "s3://other/part.parquet", "s3://warehouse/"} {
						p.ExternalFile = invalid
						require.Error(t, s.InspectParquetCommand(t.Context(), p))
					}
					require.Equal(t, 1, externalStore.opens["table/data/part.parquet"])
					p.ExternalFile = "s3://warehouse/table/data/part.parquet"
					p.MetadataOnly = false
					p.SampleRows = 1
					require.NoError(t, s.InspectParquetCommand(t.Context(), p))
				}
				// A real footer parse failure must reach the command caller.
				externalStore.data["table/data/part.parquet"] = []byte("invalid parquet")
				require.Error(t, s.InspectParquetCommand(t.Context(), p))
				if !single {
					require.Equal(t, 3, internalStore.opens["files/insert_log/1/2/20/_data/generated.parquet"], "continue after an external file failure")
					if format == "iceberg-table" {
						internalStore.data["files/insert_log/1/2/20/_metadata/manifest-7.avro"] = scanFixtureManifest(t, &storagemanifest.Manifest{
							ColumnGroups: []storagemanifest.ColumnGroup{
								{Columns: []string{"source_id"}, Format: format, Files: []storagemanifest.ColumnGroupFile{
									{Path: "s3://other/part.parquet"},
									{Path: uri},
								}},
								{Columns: []string{"101"}, Format: "parquet", Files: []storagemanifest.ColumnGroupFile{{Path: "generated.parquet"}}},
							},
						})
						opens := externalStore.opens["table/data/part.parquet"]
						require.EqualError(t, s.InspectParquetCommand(t.Context(), p), "manifest file failures: 1 path resolution, 1 inspection")
						require.Equal(t, opens+1, externalStore.opens["table/data/part.parquet"])
						require.Equal(t, 4, internalStore.opens["files/insert_log/1/2/20/_data/generated.parquet"])
					}
				}
			})
		}
	}
}

func TestInspectParquetAmbiguousURI(t *testing.T) {
	for _, single := range []bool{true, false} {
		t.Run(fmt.Sprintf("single=%t", single), func(t *testing.T) {
			s, internalStore, segment := newScanCommandFixture(t)
			data, err := proto.Marshal(&etcdpb.CollectionInfo{ID: 1, Schema: &schemapb.CollectionSchema{
				Name: "external", ExternalSource: "minio://minio/minio/table/metadata.json",
				ExternalSpec: `{"format":"iceberg-table"}`,
			}})
			require.NoError(t, err)
			s.client.(*scanFixtureKV).data["root/root-coord/collection/1"] = string(data)
			externalStore := &scanFixtureStore{
				data: map[string][]byte{
					"data.parquet":       scanFixtureParquet(t, []string{"id"}, [][]int64{{1}}),
					"minio/data.parquet": scanFixtureParquet(t, []string{"id"}, [][]int64{{2}, {3}}),
				},
				opens: make(map[string]int), closes: make(map[string]int),
			}
			location := externalSourceLocation{Host: "minio", SourceHost: "minio", Bucket: "minio", RootPath: "table/metadata.json"}
			patch := mockey.Mock(newExternalObjectStore).Return(externalStore, "minio", location.RootPath, location, nil).Build()
			defer patch.UnPatch()
			inspect := func(uri string) (string, error) {
				p := &InspectParquetParam{External: true, CollectionID: 1, MetadataOnly: true}
				if single {
					p.ExternalFile = uri
				} else {
					p.ManifestSegmentID = segment.ID
					internalStore.data["files/insert_log/1/2/20/_metadata/manifest-7.avro"] = scanFixtureManifest(t, &storagemanifest.Manifest{
						ColumnGroups: []storagemanifest.ColumnGroup{{
							Columns: []string{"id"}, Format: "iceberg-table",
							Files: []storagemanifest.ColumnGroupFile{{Path: uri, EndIndex: 1}},
						}},
					})
				}
				return captureScanOutput(t, func() error { return s.InspectParquetCommand(t.Context(), p) })
			}
			output, err := inspect("s3://minio/minio/data.parquet")
			require.Error(t, err)
			require.Contains(t, output+err.Error(), "ambiguous")
			require.Empty(t, externalStore.opens)
			output, err = inspect("minio://minio/minio/data.parquet")
			require.NoError(t, err)
			require.Contains(t, output, "Physical NumRows: 1")
			require.Equal(t, 1, externalStore.opens["data.parquet"])
			require.Zero(t, externalStore.opens["minio/data.parquet"])

			output, err = inspect("s3://minio/data.parquet")
			require.NoError(t, err)
			require.Contains(t, output, "Physical NumRows: 1")
			require.Equal(t, 2, externalStore.opens["data.parquet"])
			require.Zero(t, externalStore.opens["minio/data.parquet"])
		})
	}
}

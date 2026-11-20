package ossutil

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	storagecommon "github.com/milvus-io/birdwatcher/storage/common"
)

func TestNewResolvedObjectStoreAzure(t *testing.T) {
	accountKey := base64.StdEncoding.EncodeToString(make([]byte, 32))
	t.Setenv("AZURE_STORAGE_CONNECTION_STRING", fmt.Sprintf(
		"DefaultEndpointsProtocol=http;AccountName=test;AccountKey=%s;BlobEndpoint=http://127.0.0.1:1/test;",
		accountKey,
	))

	resolved, err := NewResolvedObjectStore(t.Context(), oss.MinioClientParam{
		CloudProvider: oss.CloudProviderAzure,
		BucketName:    "container",
		RootPath:      "root/path",
	}, oss.WithSkipCheckBucket(true))
	if err != nil {
		t.Fatalf("NewResolvedObjectStore() error = %v", err)
	}
	if resolved.Store == nil || resolved.BucketName != "container" || resolved.RootPath != "root/path" {
		t.Fatalf("NewResolvedObjectStore() = %#v", resolved)
	}
}

// resolverTestStore is a minimal oss.ObjectStore stub for path-routing tests.
// It is never opened, only passed through by ManifestPathResolver.
type resolverTestStore struct{}

func (s *resolverTestStore) Open(ctx context.Context, key string, opts ...oss.OpenOption) (storagecommon.ReadSeeker, error) {
	return nil, nil
}

func (s *resolverTestStore) Stat(ctx context.Context, key string) (*models.FsStat, error) {
	return nil, nil
}

func (s *resolverTestStore) List(ctx context.Context, prefix string, recursive bool) (<-chan oss.ObjectInfo, error) {
	return nil, nil
}

func TestManifestPathResolverMixedManifest(t *testing.T) {
	location := ExternalSourceLocation{
		Host:     "oss-cn-hangzhou.aliyuncs.com",
		Bucket:   "test-oss-0815",
		RootPath: "testlake/parquet",
	}
	internalStore := &resolverTestStore{}
	externalStore := &resolverTestStore{}

	// manifestBasePath mirrors {rootPath}/files/insert_log/{coll}/{part}/{seg}
	resolver := NewManifestPathResolver(internalStore, "by-dev/files/insert_log/1/2/3", externalStore, location)

	tests := []struct {
		name      string
		file      string
		dirPrefix string
		wantStore *resolverTestStore
		wantKey   string
		wantBack  FileBackend
		wantErr   bool
	}{
		{
			name:      "external absolute uri",
			file:      "oss://oss-cn-hangzhou.aliyuncs.com/test-oss-0815/testlake/parquet/part-0001.parquet",
			dirPrefix: "_data",
			wantStore: externalStore,
			wantKey:   "testlake/parquet/part-0001.parquet",
			wantBack:  FileBackendExternal,
		},
		{
			name:      "external ROOT_PATH placeholder",
			file:      "ROOT_PATH/part-0002.parquet",
			dirPrefix: "_data",
			wantStore: externalStore,
			wantKey:   "testlake/parquet/part-0002.parquet",
			wantBack:  FileBackendExternal,
		},
		{
			name:      "internal function output file",
			file:      "abc-123.parquet",
			dirPrefix: "_data",
			wantStore: internalStore,
			wantKey:   "by-dev/files/insert_log/1/2/3/_data/abc-123.parquet",
			wantBack:  FileBackendInternal,
		},
		{
			name:      "internal bm25 stats",
			file:      "bm25.3/0",
			dirPrefix: "_stats",
			wantStore: internalStore,
			wantKey:   "by-dev/files/insert_log/1/2/3/_stats/bm25.3/0",
			wantBack:  FileBackendInternal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, key, backend, err := resolver.Resolve(tt.file, tt.dirPrefix)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Resolve() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if store != tt.wantStore {
				t.Fatalf("Resolve() store mismatch: got %#v, want %#v", store, tt.wantStore)
			}
			if key != tt.wantKey {
				t.Fatalf("Resolve() key = %q, want %q", key, tt.wantKey)
			}
			if backend != tt.wantBack {
				t.Fatalf("Resolve() backend = %v, want %v", backend, tt.wantBack)
			}
		})
	}
}

func TestParseExternalSpecFullKeys(t *testing.T) {
	raw := `{
		"format": "parquet",
		"extfs": {
			"cloud_provider": "aws",
			"region": "us-east-1",
			"role_arn": "arn:aws:iam::1:role/r",
			"session_name": "birdwatcher",
			"external_id": "ext-123",
			"access_key_id": "AKIA",
			"access_key_value": "secret",
			"use_iam": "false",
			"iam_endpoint": "https://sts.example.com",
			"bucket_name": "override-bucket",
			"storage_type": "remote",
			"use_ssl": "true",
			"load_frequency": "3600"
		}
	}`

	spec, err := ParseExternalSpec(raw)
	if err != nil {
		t.Fatalf("ParseExternalSpec() error = %v", err)
	}
	if spec.Format != "parquet" {
		t.Fatalf("unexpected format: %s", spec.Format)
	}
	if spec.CloudProvider != "aws" {
		t.Fatalf("unexpected cloud provider: %s", spec.CloudProvider)
	}
	if spec.Region != "us-east-1" {
		t.Fatalf("unexpected region: %s", spec.Region)
	}
	if spec.RoleARN != "arn:aws:iam::1:role/r" {
		t.Fatalf("unexpected role arn: %s", spec.RoleARN)
	}
	if spec.RoleSessionName != "birdwatcher" {
		t.Fatalf("unexpected session name: %s", spec.RoleSessionName)
	}
	if spec.ExternalID != "ext-123" {
		t.Fatalf("unexpected external id: %s", spec.ExternalID)
	}
	if spec.AccessKeyID != "AKIA" || spec.AccessKeyValue != "secret" {
		t.Fatalf("unexpected access keys: %q/%q", spec.AccessKeyID, spec.AccessKeyValue)
	}
	if spec.UseIAM {
		t.Fatalf("expected use_iam=false")
	}
	if spec.IAMEndpoint != "https://sts.example.com" {
		t.Fatalf("unexpected iam endpoint: %s", spec.IAMEndpoint)
	}
	if spec.BucketName != "override-bucket" {
		t.Fatalf("unexpected bucket name: %s", spec.BucketName)
	}
	if spec.StorageType != "remote" {
		t.Fatalf("unexpected storage type: %s", spec.StorageType)
	}
	if spec.UseSSL == nil || !*spec.UseSSL {
		t.Fatalf("expected use_ssl=true, got %#v", spec.UseSSL)
	}
	if spec.LoadFrequency != 3600 {
		t.Fatalf("unexpected load frequency: %d", spec.LoadFrequency)
	}
}

func TestNewResolvedExternalObjectStoreAzureAnonymous(t *testing.T) {
	spec, err := ParseExternalSpec(`{
		"format":"parquet",
		"extfs":{
			"cloud_provider":"Azure",
			"anonymous":"true"
		}
	}`)
	if err != nil {
		t.Fatalf("ParseExternalSpec() error = %v", err)
	}
	if spec.CloudProvider != oss.CloudProviderAzure {
		t.Fatalf("cloud provider = %q, want %q", spec.CloudProvider, oss.CloudProviderAzure)
	}

	resolved, location, err := NewResolvedExternalObjectStore(
		t.Context(),
		"azure://account.blob.core.windows.net/container/root/path",
		spec,
		true,
	)
	if err != nil {
		t.Fatalf("NewResolvedExternalObjectStore() error = %v", err)
	}
	if resolved.Store == nil || resolved.BucketName != "container" || resolved.RootPath != "root/path" {
		t.Fatalf("resolved store = %#v", resolved)
	}
	if location.Address != "account.blob.core.windows.net" || location.Bucket != "container" {
		t.Fatalf("location = %#v", location)
	}
}

func TestNewResolvedExternalObjectStoreAzureBroker(t *testing.T) {
	spec, err := ParseExternalSpec(`{
		"format":"parquet",
		"extfs":{
			"cloud_provider":"azure",
			"region":"westus3",
			"access_key_id":"storage-account",
			"azure_client_id":"client-id",
			"azure_tenant_id":"tenant-id",
			"azure_credential_endpoint":"https://broker.example.com/v1/credentials/assume-role",
			"load_frequency":"3600",
			"use_ssl":true,
			"future_option":"enabled"
		}
	}`)
	if err != nil {
		t.Fatalf("ParseExternalSpec() error = %v", err)
	}
	if spec.AzureClientID != "client-id" || spec.AzureTenantID != "tenant-id" ||
		spec.AzureCredentialEndpoint != "https://broker.example.com/v1/credentials/assume-role" {
		t.Fatalf("Azure broker spec = %#v", spec)
	}

	resolved, location, err := NewResolvedExternalObjectStore(
		t.Context(),
		"azure://core.windows.net/container/root/path",
		spec,
		true,
	)
	if err != nil {
		t.Fatalf("NewResolvedExternalObjectStore() error = %v", err)
	}
	if resolved.Store == nil || resolved.BucketName != "container" || resolved.RootPath != "root/path" {
		t.Fatalf("resolved store = %#v", resolved)
	}
	if location.Address != "core.windows.net" || location.Bucket != "container" || location.RootPath != "root/path" {
		t.Fatalf("location = %#v", location)
	}
}

func TestParseExternalSpecEmpty(t *testing.T) {
	spec, err := ParseExternalSpec("")
	if err != nil {
		t.Fatalf("ParseExternalSpec() error = %v", err)
	}
	if spec.Format != "" {
		t.Fatalf("unexpected format: %s", spec.Format)
	}
}

func TestIsLegacyExternalSpec(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want bool
	}{
		{
			name: "omitted extfs",
			raw:  `{"format":"parquet"}`,
			want: true,
		},
		{
			name: "empty extfs",
			raw:  `{"format":"parquet","extfs":{}}`,
			want: true,
		},
		{
			name: "configured extfs",
			raw:  `{"format":"parquet","extfs":{"cloud_provider":"aws"}}`,
			want: false,
		},
		{
			name: "Azure broker extensions",
			raw: `{
				"format":"parquet",
				"extfs":{
					"azure_client_id":"client-id",
					"azure_tenant_id":"tenant-id",
					"azure_credential_endpoint":"https://broker.example.com/credentials"
				}
			}`,
			want: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec, err := ParseExternalSpec(test.raw)
			if err != nil {
				t.Fatalf("ParseExternalSpec() error = %v", err)
			}
			if got := IsLegacyExternalSpec(spec); got != test.want {
				t.Fatalf("IsLegacyExternalSpec() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestParseExternalSpecRejectsUnsupportedFormat(t *testing.T) {
	raw := `{"format":"lance-table","extfs":{}}`
	if _, err := ParseExternalSpec(raw); err == nil {
		t.Fatalf("expected error for unsupported format")
	}
}

func TestParseExternalSource(t *testing.T) {
	location, err := ParseExternalSource("oss://oss-cn-hangzhou.aliyuncs.com/test-oss-0815/testlake/parquet")
	if err != nil {
		t.Fatalf("ParseExternalSource() error = %v", err)
	}
	if location.Scheme != "oss" {
		t.Fatalf("unexpected scheme: %s", location.Scheme)
	}
	if location.Host != "oss-cn-hangzhou.aliyuncs.com" {
		t.Fatalf("unexpected host: %s", location.Host)
	}
	if location.Bucket != "test-oss-0815" {
		t.Fatalf("unexpected bucket: %s", location.Bucket)
	}
	if location.RootPath != "testlake/parquet" {
		t.Fatalf("unexpected root path: %s", location.RootPath)
	}
}

func TestResolveExternalObjectKey(t *testing.T) {
	location := ExternalSourceLocation{
		Host:     "oss-cn-hangzhou.aliyuncs.com",
		Bucket:   "test-oss-0815",
		RootPath: "testlake/parquet",
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
			got, err := ResolveExternalObjectKey(location, tt.file)
			if err != nil {
				t.Fatalf("ResolveExternalObjectKey() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("ResolveExternalObjectKey() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestInferCloudProviderFromScheme(t *testing.T) {
	tests := []struct {
		scheme string
		want   string
	}{
		{scheme: "oss", want: "aliyun"},
		{scheme: "s3", want: "aws"},
		{scheme: "aws", want: "aws"},
		{scheme: "minio", want: "aws"},
		{scheme: "gs", want: "gcp"},
		{scheme: "gcs", want: "gcp"},
		{scheme: "cos", want: "tencent"},
		{scheme: "obs", want: "huawei"},
		{scheme: "unknown", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.scheme, func(t *testing.T) {
			if got := InferCloudProviderFromScheme(tt.scheme); got != tt.want {
				t.Fatalf("InferCloudProviderFromScheme(%q) = %q, want %q", tt.scheme, got, tt.want)
			}
		})
	}
}

func TestResolveExternalSourceAWSForm(t *testing.T) {
	// s3://bucket/key with AWS cloud_provider + region: host is the bucket,
	// endpoint is derived.
	location, err := ResolveExternalSource("s3://my-bucket/root/file", ExternalSourceSpec{
		CloudProvider: "aws",
		Region:        "us-east-1",
	})
	if err != nil {
		t.Fatalf("ResolveExternalSource() error = %v", err)
	}
	if location.Form != LocationFormAWS {
		t.Fatalf("expected AWS form, got %v", location.Form)
	}
	if location.Bucket != "my-bucket" {
		t.Fatalf("unexpected bucket: %s", location.Bucket)
	}
	if location.RootPath != "root/file" {
		t.Fatalf("unexpected root path: %s", location.RootPath)
	}
	if location.Address != "s3.us-east-1.amazonaws.com" {
		t.Fatalf("unexpected address: %s", location.Address)
	}
}

func TestResolveExternalSourceMilvusForm(t *testing.T) {
	// oss://endpoint/bucket/key stays Milvus form regardless of spec.
	location, err := ResolveExternalSource("oss://oss-cn-hangzhou.aliyuncs.com/test-oss-0815/testlake/parquet",
		ExternalSourceSpec{CloudProvider: "aliyun", Region: "cn-hangzhou"})
	if err != nil {
		t.Fatalf("ResolveExternalSource() error = %v", err)
	}
	if location.Form != LocationFormMilvus {
		t.Fatalf("expected Milvus form, got %v", location.Form)
	}
	if location.Bucket != "test-oss-0815" {
		t.Fatalf("unexpected bucket: %s", location.Bucket)
	}
	if location.RootPath != "testlake/parquet" {
		t.Fatalf("unexpected root path: %s", location.RootPath)
	}
	if location.Address != "oss-cn-hangzhou.aliyuncs.com" {
		t.Fatalf("unexpected address: %s", location.Address)
	}
}

func TestResolveExternalSourceCloudEndpointHostStaysMilvus(t *testing.T) {
	// s3://s3.amazonaws.com/bucket/key: host is a known cloud endpoint, so it
	// stays Milvus form even with a regional spec.
	location, err := ResolveExternalSource("s3://s3.amazonaws.com/my-bucket/key",
		ExternalSourceSpec{CloudProvider: "aws", Region: "us-east-1"})
	if err != nil {
		t.Fatalf("ResolveExternalSource() error = %v", err)
	}
	if location.Form != LocationFormMilvus {
		t.Fatalf("expected Milvus form, got %v", location.Form)
	}
	if location.Bucket != "my-bucket" {
		t.Fatalf("unexpected bucket: %s", location.Bucket)
	}
}

func TestResolveExternalObjectKeyAWSForm(t *testing.T) {
	location := ExternalSourceLocation{
		Scheme:   "s3",
		Host:     "my-bucket",
		Bucket:   "my-bucket",
		RootPath: "root",
		Address:  "s3.us-east-1.amazonaws.com",
		Form:     LocationFormAWS,
	}

	tests := []struct {
		name string
		file string
		want string
	}{
		{
			name: "absolute aws uri",
			file: "s3://my-bucket/root/data/part-0001.parquet",
			want: "root/data/part-0001.parquet",
		},
		{
			name: "relative path",
			file: "data/part-0002.parquet",
			want: "root/data/part-0002.parquet",
		},
		{
			name: "mismatched bucket",
			file: "s3://other-bucket/root/data.parquet",
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ResolveExternalObjectKey(location, tt.file)
			if tt.want == "" {
				if err == nil {
					t.Fatalf("expected error for %q, got key %q", tt.file, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolveExternalObjectKey() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("ResolveExternalObjectKey() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestParseExternalSpecCompatibleMetadata(t *testing.T) {
	for _, raw := range []string{
		`{"format":"parquet","extfs":{"use_iam":true,"use_ssl":false,"anonymous":false,"use_virtual_host":true,"load_frequency":3600}}`,
		`{"format":"PARQUET","extfs":{"use_iam":"TRUE","use_ssl":"false","anonymous":"false","use_virtual_host":"true","load_frequency":"3600"}}`,
	} {
		spec, err := ParseExternalSpec(raw)
		if err != nil {
			t.Fatal(err)
		}
		if spec.Format != "parquet" || !spec.UseIAM || spec.Anonymous || spec.UseSSL == nil || *spec.UseSSL ||
			spec.UseVirtualHost == nil || !*spec.UseVirtualHost || spec.LoadFrequency != 3600 {
			t.Fatalf("incorrect normalized properties: %+v", spec)
		}
		if spec.Extfs["use_iam"] != "true" || spec.Extfs["use_ssl"] != "false" {
			t.Fatalf("boolean properties were not normalized: %v", spec.Extfs)
		}
	}
}

func TestParseExternalSpecUnknownKeys(t *testing.T) {
	spec, err := ParseExternalSpec(`{"extfs":{"cloud_provider":"aws","region":"us-east-1","use_iam":true,"future_option":"enabled","future_object":{"enabled":true}}}`)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Format != "parquet" || spec.CloudProvider != "aws" || spec.Region != "us-east-1" ||
		spec.Extfs["future_option"] != "enabled" || spec.Extfs["future_object"] != `{"enabled":true}` {
		t.Fatalf("metadata was not preserved: %+v", spec)
	}
	if IsLegacyExternalSpec(spec) {
		t.Fatal("configured extfs must not fall back to local storage credentials")
	}
	if err := ValidateExternalStorageSpec("s3://bucket/data", spec); err != nil {
		t.Fatalf("unknown keys blocked storage validation: %v", err)
	}
}

func TestParseExternalSpecCompatibilityValidation(t *testing.T) {
	for _, raw := range []string{
		`{"extfs":[]}`,
		`{"format":12}`,
		`{"extfs":{"use_ssl":"invalid"}}`,
		`{"format":"lance-table","extfs":{"future_option":true}}`,
		`{"extfs":{"load_frequency":-1,"future_option":true}}`,
	} {
		if _, err := ParseExternalSpec(raw); err == nil {
			t.Fatalf("expected error for %s", raw)
		}
	}
	spec, err := ParseExternalSpecLoose(`{"format":"future-format","extfs":{"future_option":true}}`)
	if err != nil || spec.Format != "future-format" || spec.Extfs["future_option"] != "true" {
		t.Fatalf("loose parsing failed: %+v, %v", spec, err)
	}
}

func TestResolveExternalObjectURI(t *testing.T) {
	for _, form := range []LocationForm{LocationFormAWS, LocationFormMilvus} {
		location := ExternalSourceLocation{
			Form: form, Host: "warehouse", Address: "storage.example:9000",
			Bucket: "warehouse", RootPath: "table/metadata/v18.json",
		}
		tests := []struct {
			uri  string
			want string
		}{
			{"s3://warehouse/table/data/part.parquet", "table/data/part.parquet"},
			{"s3://storage.example:9000/warehouse/table/data/part.parquet", "table/data/part.parquet"},
			{"s3://warehouse/ROOT_PATH/data.parquet", "ROOT_PATH/data.parquet"},
			{"s3://warehouse/data/a%3Fb%23c%25.parquet", "data/a?b#c%.parquet"},
			{"s3://warehouse/data/a%2Fb.parquet", "data/a/b.parquet"},
			{"s3://warehouse/data/a..b.parquet", "data/a..b.parquet"},
			{"s3://warehouse//data/part.parquet", ""},
			{"s3://warehouse/data//part.parquet", ""},
			{"s3://warehouse/data/../part.parquet", ""},
			{"s3://warehouse/data/%2Fpart.parquet", ""},
			{"s3://storage.example:9000/warehouse/data/%2e%2e/part.parquet", ""},
			{"data/part.parquet", ""},
			{"ROOT_PATH/part.parquet", ""},
			{"//warehouse/data/part.parquet", ""},
			{"s3:///data/part.parquet", ""},
			{"s3://other/part.parquet", ""},
			{"s3://storage.example:9000/other/part.parquet", ""},
			{"s3://storage.example:9000/warehouse%2Fpart.parquet", ""},
			{"s3://warehouse/", ""},
			{"s3://storage.example:9000/warehouse/", ""},
			{"s3://warehouse/a%zz", ""},
			{"s3://warehouse/a?versionId=old", ""},
			{"s3://warehouse/a?", ""},
			{"s3://warehouse/a#fragment", ""},
			{"s3://warehouse/a#", ""},
			{"s3://user:secret@warehouse/a", ""},
		}
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%v/%s", form, tt.uri), func(t *testing.T) {
				got, err := ResolveExternalObjectURI(location, tt.uri)
				if tt.want == "" {
					if err == nil {
						t.Fatalf("expected URI validation error, got %q", got)
					}
					return
				}
				if err != nil || got != tt.want {
					t.Fatalf("got %q, %v; want %q", got, err, tt.want)
				}
			})
		}
	}
}

func TestResolveExternalObjectURIAmbiguousHost(t *testing.T) {
	for _, form := range []LocationForm{LocationFormAWS, LocationFormMilvus} {
		location := ExternalSourceLocation{Form: form, Host: "minio", Address: "minio", Bucket: "minio"}
		for _, uri := range []string{"s3://minio/minio/data.parquet", "s3://minio/%6Dinio/data.parquet"} {
			if _, err := ResolveExternalObjectURI(location, uri); err == nil || !strings.Contains(err.Error(), "ambiguous") {
				t.Errorf("URI %q: got %v, want ambiguity error", uri, err)
			}
		}
		for _, tt := range []struct {
			uri  string
			want string
		}{
			{"s3://minio/data.parquet", "data.parquet"},
			{"s3://minio/minio", "minio"},
			{"s3://minio/minio/", "minio/"},
			{"s3://minio/minio%2Fdata.parquet", "minio/data.parquet"},
		} {
			key, err := ResolveExternalObjectURI(location, tt.uri)
			if err != nil || key != tt.want {
				t.Errorf("URI %q: got %q, %v; want %q", tt.uri, key, err, tt.want)
			}
		}
		key, err := ResolveExternalObjectURI(location, "minio://minio/minio/data.parquet")
		if err != nil || key != "data.parquet" {
			t.Errorf("explicit endpoint URI: got %q, %v", key, err)
		}
		if _, err := ResolveExternalObjectURI(location, "minio://minio/other/data.parquet"); err == nil {
			t.Error("explicit endpoint URI must validate the bucket")
		}
	}
	location := ExternalSourceLocation{Form: LocationFormAWS, Host: "bucket", Address: "endpoint", Bucket: "bucket"}
	if _, err := ResolveExternalObjectURI(location, "minio://bucket/data.parquet"); err == nil {
		t.Error("minio scheme requires an endpoint host even when it matches the bucket")
	}
}

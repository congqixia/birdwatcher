package ossutil

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
)

// ExternalSourceSpec is the parsed external collection specification
// (schema.ExternalSpec). extfs keys mirror milvus
// pkg/util/externalspec/specutil.ExtfsKey*.
type ExternalSourceSpec struct {
	Format                  string
	Extfs                   map[string]string
	CloudProvider           string
	Region                  string
	RoleARN                 string
	RoleSessionName         string
	ExternalID              string
	AliyunRoleAuthMode      string
	AccessKeyID             string
	AccessKeyValue          string
	UseIAM                  bool
	IAMEndpoint             string
	UseSSL                  *bool
	UseVirtualHost          *bool
	Anonymous               bool
	BucketName              string
	StorageType             string
	GCPTargetServiceAccount string
	SSLCACert               string
	LoadFrequency           int
	AzureClientID           string
	AzureTenantID           string
	AzureCredentialEndpoint string
}

// LocationForm distinguishes the two accepted external_source URI shapes,
// mirroring milvus pkg/util/externalspec and milvus-storage StorageUri.
type LocationForm int

const (
	// LocationFormMilvus is `scheme://endpoint/bucket/key`: the URI host is the
	// storage endpoint and the first path segment is the bucket.
	LocationFormMilvus LocationForm = iota
	// LocationFormAWS is `scheme://bucket/key`: the URI host is the bucket and
	// the endpoint is derived from extfs.cloud_provider + extfs.region.
	LocationFormAWS
)

// ExternalSourceLocation is the parsed external_source URI.
type ExternalSourceLocation struct {
	Scheme   string
	Host     string
	Bucket   string
	RootPath string
	// Address is the storage endpoint used to build the object store client:
	// the URI host for the Milvus form, or externalspec.DeriveEndpoint(provider, region)
	// for the AWS form.
	Address string
	Form    LocationForm
}

// ParseExternalSpec parses the external spec JSON (schema.ExternalSpec).
func ParseExternalSpec(raw string) (ExternalSourceSpec, error) {
	return parseExternalSpec(raw, true)
}

// ParseExternalSpecLoose parses the external spec JSON without rejecting
// unsupported file formats. It is used by commands that only annotate paths
// (e.g. show manifest) and must not fail on non-parquet external collections.
func ParseExternalSpecLoose(raw string) (ExternalSourceSpec, error) {
	return parseExternalSpec(raw, false)
}

func parseExternalSpec(raw string, validateFormat bool) (ExternalSourceSpec, error) {
	if strings.TrimSpace(raw) == "" {
		return ExternalSourceSpec{}, nil
	}

	normalized, extensions, err := extractExternalSpecExtensions(raw)
	if err != nil {
		return ExternalSourceSpec{}, err
	}
	parsed, err := externalspec.ParseExternalSpec(normalized)
	if err != nil {
		parsed, err = parseCompatibleExternalSpec(normalized)
		if err != nil {
			return ExternalSourceSpec{}, err
		}
	}
	if validateFormat && parsed.Format != "" && parsed.Format != externalspec.FormatParquet {
		return ExternalSourceSpec{}, fmt.Errorf("external collection format %s is not supported", parsed.Format)
	}

	extfs := parsed.Extfs
	spec := ExternalSourceSpec{
		Format:                  parsed.Format,
		Extfs:                   extfs,
		CloudProvider:           strings.ToLower(strings.TrimSpace(extfs[externalspec.ExtfsKeyCloudProvider])),
		Region:                  extfs[externalspec.ExtfsKeyRegion],
		RoleARN:                 extfs[externalspec.ExtfsKeyRoleARN],
		RoleSessionName:         extfs[externalspec.ExtfsKeySessionName],
		ExternalID:              extfs[externalspec.ExtfsKeyExternalID],
		AliyunRoleAuthMode:      extensions.aliyunRoleAuthMode,
		AccessKeyID:             extfs[externalspec.ExtfsKeyAccessKeyID],
		AccessKeyValue:          extfs[externalspec.ExtfsKeyAccessKeyValue],
		UseIAM:                  extfs[externalspec.ExtfsKeyUseIAM] == "true",
		IAMEndpoint:             extfs[externalspec.ExtfsKeyIAMEndpoint],
		Anonymous:               extfs[externalspec.ExtfsKeyAnonymous] == "true",
		BucketName:              extfs[externalspec.ExtfsKeyBucketName],
		StorageType:             extfs[externalspec.ExtfsKeyStorageType],
		GCPTargetServiceAccount: extfs[externalspec.ExtfsKeyGCPTargetServiceAccount],
		SSLCACert:               extfs[externalspec.ExtfsKeySSLCACert],
		AzureClientID:           extensions.azureClientID,
		AzureTenantID:           extensions.azureTenantID,
		AzureCredentialEndpoint: extensions.azureCredentialEndpoint,
	}
	if rawUseSSL, ok := extfs[externalspec.ExtfsKeyUseSSL]; ok {
		useSSL := rawUseSSL == "true"
		spec.UseSSL = &useSSL
	}
	if rawUseVirtualHost, ok := extfs[externalspec.ExtfsKeyUseVirtualHost]; ok {
		useVirtualHost := rawUseVirtualHost == "true"
		spec.UseVirtualHost = &useVirtualHost
	}
	if rawLoadFrequency := extfs[externalspec.ExtfsKeyLoadFrequency]; rawLoadFrequency != "" {
		loadFrequency, err := strconv.Atoi(rawLoadFrequency)
		if err != nil || loadFrequency <= 0 {
			return ExternalSourceSpec{}, fmt.Errorf(
				"extfs.%s must be a positive integer, got %q",
				externalspec.ExtfsKeyLoadFrequency,
				rawLoadFrequency,
			)
		}
		spec.LoadFrequency = loadFrequency
	}
	return spec, nil
}

// parseCompatibleExternalSpec reads persisted metadata that predates or is newer
// than the pinned Milvus parser. Unknown extfs keys are retained; storage and
// command-specific validation still runs after parsing.
func parseCompatibleExternalSpec(raw string) (*externalspec.ExternalSpec, error) {
	var payload struct {
		Format string                     `json:"format"`
		Extfs  map[string]json.RawMessage `json:"extfs"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil, fmt.Errorf("parse external spec: %w", err)
	}
	parsed := &externalspec.ExternalSpec{
		Format: strings.ToLower(strings.TrimSpace(payload.Format)),
		Extfs:  make(map[string]string, len(payload.Extfs)),
	}
	if parsed.Format == "" {
		parsed.Format = externalspec.FormatParquet
	}
	for key, rawValue := range payload.Extfs {
		var value string
		if err := json.Unmarshal(rawValue, &value); err != nil {
			value = string(rawValue)
		}
		switch key {
		case externalspec.ExtfsKeyUseIAM, externalspec.ExtfsKeyUseSSL,
			externalspec.ExtfsKeyUseVirtualHost, externalspec.ExtfsKeyAnonymous:
			boolean, err := strconv.ParseBool(strings.TrimSpace(value))
			if err != nil {
				return nil, fmt.Errorf("extfs.%s must be a boolean", key)
			}
			value = strconv.FormatBool(boolean)
		}
		parsed.Extfs[key] = value
	}
	return parsed, nil
}

type externalSpecExtensions struct {
	aliyunRoleAuthMode      string
	azureClientID           string
	azureTenantID           string
	azureCredentialEndpoint string
}

// extractExternalSpecExtensions removes fields that are newer than the pinned
// Milvus parser, then returns them separately for Birdwatcher's storage layer.
// Aliyun role mode may also appear at the top level in legacy metadata.
func extractExternalSpecExtensions(raw string) (string, externalSpecExtensions, error) {
	const aliyunRoleAuthModeKey = "aliyun_role_auth_mode"
	var payload map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return "", externalSpecExtensions{}, fmt.Errorf("parse external spec: %w", err)
	}

	readString := func(field string, value json.RawMessage) (string, error) {
		if len(value) == 0 {
			return "", nil
		}
		var result string
		if err := json.Unmarshal(value, &result); err != nil {
			return "", fmt.Errorf("%s must be a string: %w", field, err)
		}
		return strings.TrimSpace(result), nil
	}
	readMode := func(value json.RawMessage) (string, error) {
		result, err := readString(aliyunRoleAuthModeKey, value)
		return strings.ToLower(result), err
	}

	extensions := externalSpecExtensions{}
	mode, err := readMode(payload[aliyunRoleAuthModeKey])
	if err != nil {
		return "", externalSpecExtensions{}, err
	}
	extensions.aliyunRoleAuthMode = mode
	if rawExtfs, ok := payload["extfs"]; ok && len(rawExtfs) > 0 && string(rawExtfs) != "null" {
		var extfs map[string]json.RawMessage
		if err := json.Unmarshal(rawExtfs, &extfs); err != nil {
			return "", externalSpecExtensions{}, fmt.Errorf("parse external spec extfs: %w", err)
		}
		if rawMode, ok := extfs[aliyunRoleAuthModeKey]; ok {
			if extensions.aliyunRoleAuthMode == "" {
				extensions.aliyunRoleAuthMode, err = readMode(rawMode)
				if err != nil {
					return "", externalSpecExtensions{}, err
				}
			}
			delete(extfs, aliyunRoleAuthModeKey)
		}
		for field, target := range map[string]*string{
			"azure_client_id":           &extensions.azureClientID,
			"azure_tenant_id":           &extensions.azureTenantID,
			"azure_credential_endpoint": &extensions.azureCredentialEndpoint,
		} {
			if rawValue, ok := extfs[field]; ok {
				*target, err = readString(field, rawValue)
				if err != nil {
					return "", externalSpecExtensions{}, err
				}
				delete(extfs, field)
			}
		}
		normalizedExtfs, err := json.Marshal(extfs)
		if err != nil {
			return "", externalSpecExtensions{}, fmt.Errorf("normalize external spec extfs: %w", err)
		}
		payload["extfs"] = normalizedExtfs
	}

	normalized, err := json.Marshal(payload)
	if err != nil {
		return "", externalSpecExtensions{}, fmt.Errorf("normalize external spec: %w", err)
	}
	return string(normalized), extensions, nil
}

// ValidateExternalStorageSpec validates the shared external storage contract,
// including Birdwatcher's Azure credential broker extension.
func ValidateExternalStorageSpec(source string, spec ExternalSourceSpec) error {
	if !hasAzureCredentialBrokerSpec(spec) {
		return externalspec.ValidateExtfsComplete(source, spec.Extfs)
	}
	if err := externalspec.ValidateExternalSource(source); err != nil {
		return err
	}
	u, err := url.Parse(source)
	if err != nil {
		return err
	}
	if !strings.EqualFold(u.Scheme, externalspec.SchemeAzure) ||
		!strings.EqualFold(spec.CloudProvider, externalspec.CloudProviderAzure) {
		return fmt.Errorf("azure credential broker requires scheme=azure and extfs.cloud_provider=azure")
	}
	required := []struct {
		name  string
		value string
	}{
		{name: "access_key_id", value: spec.AccessKeyID},
		{name: "region", value: spec.Region},
		{name: "azure_client_id", value: spec.AzureClientID},
		{name: "azure_tenant_id", value: spec.AzureTenantID},
		{name: "azure_credential_endpoint", value: spec.AzureCredentialEndpoint},
	}
	for _, field := range required {
		if field.value == "" {
			return fmt.Errorf("extfs.%s is required for Azure credential broker mode", field.name)
		}
	}
	if spec.AccessKeyValue != "" || spec.RoleARN != "" || spec.UseIAM ||
		spec.GCPTargetServiceAccount != "" || spec.Anonymous {
		return fmt.Errorf("azure credential broker cannot be combined with another credential mode")
	}
	endpoint, err := url.Parse(spec.AzureCredentialEndpoint)
	if err != nil || endpoint.Host == "" ||
		(endpoint.Scheme != "http" && endpoint.Scheme != "https") {
		return fmt.Errorf("extfs.azure_credential_endpoint must be a valid HTTP(S) URL")
	}
	return nil
}

// IsLegacyExternalSpec reports whether an external collection predates the
// self-contained extfs storage configuration. An explicitly empty extfs object
// is equivalent to an omitted one; Azure broker extensions are self-contained
// even after their fields are removed from the canonical extfs map.
func IsLegacyExternalSpec(spec ExternalSourceSpec) bool {
	return len(spec.Extfs) == 0 && !hasAzureCredentialBrokerSpec(spec)
}

func hasAzureCredentialBrokerSpec(spec ExternalSourceSpec) bool {
	return spec.AzureClientID != "" || spec.AzureTenantID != "" || spec.AzureCredentialEndpoint != ""
}

// ParseExternalSource parses the external_source URI into scheme / host /
// bucket / root path, assuming the Milvus form (host = endpoint, first path
// segment = bucket). Use ResolveExternalSource to apply the AWS-form swap
// based on the spec.
func ParseExternalSource(raw string) (ExternalSourceLocation, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return ExternalSourceLocation{}, fmt.Errorf("parse external source: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return ExternalSourceLocation{}, fmt.Errorf("invalid external source: %s", raw)
	}
	parts := strings.Split(strings.Trim(strings.TrimSpace(u.Path), "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		return ExternalSourceLocation{}, fmt.Errorf("external source %s does not include bucket", raw)
	}
	location := ExternalSourceLocation{
		Scheme:   strings.ToLower(u.Scheme),
		Host:     u.Host,
		Bucket:   parts[0],
		RootPath: "",
		Address:  u.Host,
		Form:     LocationFormMilvus,
	}
	if len(parts) > 1 {
		location.RootPath = path.Join(parts[1:]...)
		if location.RootPath == "." {
			location.RootPath = ""
		}
	}
	return location, nil
}

// ResolveExternalSource applies the two-form URI contract on top of
// ParseExternalSource. It mirrors milvus-storage's Layer-3 swap decision:
//
//	derived := externalspec.DeriveEndpoint(spec.CloudProvider, spec.Region)
//	if derived != "" && StripURIScheme(derived) != host &&
//	   !externalspec.IsCloudEndpointHost(host) {
//	    // AWS form: host is the bucket, endpoint is derived
//	}
//
// The returned location carries the final bucket/root path and the endpoint
// address used to build the object store client.
func ResolveExternalSource(raw string, spec ExternalSourceSpec) (ExternalSourceLocation, error) {
	location, err := ParseExternalSource(raw)
	if err != nil {
		return ExternalSourceLocation{}, err
	}

	derived := externalspec.DeriveEndpoint(spec.CloudProvider, spec.Region)
	derivedAddress := StripURIScheme(derived)
	if derivedAddress != "" && !strings.EqualFold(derivedAddress, location.Host) &&
		!externalspec.IsCloudEndpointHost(location.Host) {
		// AWS form: host is the bucket, path is the key (may be empty).
		location.Form = LocationFormAWS
		location.Bucket = location.Host
		u, err := url.Parse(raw)
		if err != nil {
			return ExternalSourceLocation{}, fmt.Errorf("parse external source: %w", err)
		}
		trimmed := strings.TrimPrefix(u.Path, "/")
		if trimmed != "" {
			location.RootPath = path.Clean(trimmed)
			if location.RootPath == "." {
				location.RootPath = ""
			}
		} else {
			location.RootPath = ""
		}
		location.Address = derivedAddress
	}
	return location, nil
}

// StripURIScheme removes an http(s):// prefix, if present.
func StripURIScheme(addr string) string {
	for _, scheme := range []string{"https://", "http://"} {
		if strings.HasPrefix(addr, scheme) {
			return strings.TrimPrefix(addr, scheme)
		}
	}
	return addr
}

// InferCloudProviderFromScheme infers the cloud provider from the URI scheme.
func InferCloudProviderFromScheme(scheme string) string {
	switch strings.ToLower(strings.TrimSpace(scheme)) {
	case "oss":
		return oss.CloudProviderAliyun
	case "s3", "aws", "minio":
		return oss.CloudProviderAWS
	case "gs", "gcs":
		return oss.CloudProviderGCP
	case "cos":
		return oss.CloudProviderTencent
	case "obs":
		return oss.CloudProviderHuawei
	case "azure":
		return oss.CloudProviderAzure
	default:
		return ""
	}
}

// NewResolvedExternalObjectStore builds a ResolvedObjectStore from an external
// source URI and parsed spec. Credentials (role_arn/external_id/AK/SK/...)
// come from the collection spec, NOT from milvus minio configuration.
func NewResolvedExternalObjectStore(
	ctx context.Context,
	source string,
	spec ExternalSourceSpec,
	skipBucketCheck bool,
) (*oss.ResolvedObjectStore, ExternalSourceLocation, error) {
	if !IsLegacyExternalSpec(spec) {
		if err := ValidateExternalStorageSpec(source, spec); err != nil {
			return nil, ExternalSourceLocation{}, err
		}
	}
	location, err := ResolveExternalSource(source, spec)
	if err != nil {
		return nil, ExternalSourceLocation{}, err
	}

	provider := strings.ToLower(strings.TrimSpace(spec.CloudProvider))
	if provider == "" {
		provider = InferCloudProviderFromScheme(location.Scheme)
	}
	if provider == "" {
		return nil, ExternalSourceLocation{}, fmt.Errorf("unsupported external source scheme/provider: %s/%s", location.Scheme, spec.CloudProvider)
	}

	useSSL := true
	if spec.UseSSL != nil {
		useSSL = *spec.UseSSL
	}

	bucket := location.Bucket
	if spec.BucketName != "" {
		bucket = spec.BucketName
	}

	addr := location.Address
	if addr == "" {
		addr = location.Host
	}

	param := oss.MinioClientParam{
		Addr:                         addr,
		UseSSL:                       useSSL,
		CloudProvider:                provider,
		Region:                       spec.Region,
		AK:                           spec.AccessKeyID,
		SK:                           spec.AccessKeyValue,
		UseIAM:                       spec.UseIAM,
		Anonymous:                    spec.Anonymous,
		IAMEndpoint:                  spec.IAMEndpoint,
		UseVirtualHost:               spec.UseVirtualHost,
		RoleARN:                      spec.RoleARN,
		RoleSessionName:              spec.RoleSessionName,
		ExternalID:                   spec.ExternalID,
		LoadFrequency:                spec.LoadFrequency,
		AliyunRoleAuthMode:           spec.AliyunRoleAuthMode,
		AzureClientID:                spec.AzureClientID,
		AzureTenantID:                spec.AzureTenantID,
		AzureCredentialEndpoint:      spec.AzureCredentialEndpoint,
		AzureRequestTimeoutMs:        3000,
		DisableAzureConnectionString: provider == oss.CloudProviderAzure,
		BucketName:                   bucket,
		RootPath:                     location.RootPath,
	}
	if provider == oss.CloudProviderAliyun && param.RoleARN != "" && param.AliyunRoleAuthMode == "" {
		param.AliyunRoleAuthMode = "oidc"
	}
	// Fallback to IAM chain when no explicit role arn / static key is present.
	if param.RoleARN == "" && param.AK == "" && param.SK == "" && !param.Anonymous {
		param.UseIAM = true
	}
	oss.WithSkipCheckBucket(skipBucketCheck)(&param)

	if provider == oss.CloudProviderAzure {
		store, err := oss.NewAzureObjectStore(ctx, param)
		if err != nil {
			return nil, ExternalSourceLocation{}, err
		}
		return &oss.ResolvedObjectStore{
			Store:      store,
			BucketName: param.BucketName,
			RootPath:   param.RootPath,
		}, location, nil
	}
	client, err := oss.NewMinioClient(ctx, param)
	if err != nil {
		return nil, ExternalSourceLocation{}, err
	}
	return &oss.ResolvedObjectStore{
		Store:      oss.NewMinioObjectStore(client),
		BucketName: client.BucketName,
		RootPath:   client.RootPath,
	}, location, nil
}

// NewResolvedExternalObjectStoreFromSchema builds an external store from a raw
// external_source URI + external_spec JSON pair.
func NewResolvedExternalObjectStoreFromSchema(
	ctx context.Context,
	externalSource, externalSpec string,
	skipBucketCheck bool,
) (*oss.ResolvedObjectStore, ExternalSourceLocation, error) {
	if externalSource == "" {
		return nil, ExternalSourceLocation{}, fmt.Errorf("external source is empty")
	}
	spec, err := ParseExternalSpec(externalSpec)
	if err != nil {
		return nil, ExternalSourceLocation{}, err
	}
	return NewResolvedExternalObjectStore(ctx, externalSource, spec, skipBucketCheck)
}

// NewResolvedExternalObjectStoreFromCollection builds an external store from a
// collection model. Returns an error if the collection is not external.
func NewResolvedExternalObjectStoreFromCollection(
	ctx context.Context,
	collection *models.Collection,
	skipBucketCheck bool,
) (*oss.ResolvedObjectStore, ExternalSourceLocation, error) {
	proto := collection.GetProto()
	if proto.GetSchema().GetExternalSource() == "" {
		return nil, ExternalSourceLocation{}, fmt.Errorf("collection %d does not have external source", proto.GetID())
	}
	return NewResolvedExternalObjectStoreFromSchema(
		ctx,
		proto.GetSchema().GetExternalSource(),
		proto.GetSchema().GetExternalSpec(),
		skipBucketCheck,
	)
}

// ResolveExternalLocationFromCollection parses the collection's external source
// and spec into a location WITHOUT connecting to the external bucket or
// validating the file format. Commands that only annotate paths (e.g. show
// manifest) should use this instead of NewResolvedExternalObjectStoreFromCollection
// so they do not fail when external credentials or connectivity are broken.
func ResolveExternalLocationFromCollection(collection *models.Collection) (ExternalSourceLocation, error) {
	proto := collection.GetProto()
	schema := proto.GetSchema()
	if schema.GetExternalSource() == "" {
		return ExternalSourceLocation{}, fmt.Errorf("collection %d does not have external source", proto.GetID())
	}
	spec, err := ParseExternalSpecLoose(schema.GetExternalSpec())
	if err != nil {
		return ExternalSourceLocation{}, err
	}
	return ResolveExternalSource(schema.GetExternalSource(), spec)
}

// FileBackend indicates which object store a manifest file belongs to.
type FileBackend int

const (
	// FileBackendInternal means the file lives in milvus's own object storage.
	FileBackendInternal FileBackend = iota
	// FileBackendExternal means the file lives in the external collection's
	// source bucket and requires the collection-attached credentials.
	FileBackendExternal
)

func (b FileBackend) String() string {
	switch b {
	case FileBackendInternal:
		return "internal"
	case FileBackendExternal:
		return "external"
	default:
		return "unknown"
	}
}

// ManifestPathResolver routes a manifest file path to the correct object store.
//
// The discriminator matches milvus-storage's FilesystemCache::resolve_config:
//   - absolute URI (contains "://")  -> external store
//   - contains "ROOT_PATH"           -> external store (placeholder replaced)
//   - relative path (no scheme)      -> internal store, resolved under the
//     segment base path + dir prefix
//
// This is what makes a single manifest mixing external data files and internal
// function-output files (e.g. sparse vectors generated from varchar) work.
type ManifestPathResolver struct {
	internalStore    oss.ObjectStore
	externalStore    oss.ObjectStore
	manifestBasePath string
	location         ExternalSourceLocation
}

// NewManifestPathResolver builds a resolver. manifestBasePath is the resolved
// segment base path (e.g. {rootPath}/files/insert_log/{coll}/{part}/{seg}).
func NewManifestPathResolver(
	internalStore oss.ObjectStore,
	manifestBasePath string,
	externalStore oss.ObjectStore,
	location ExternalSourceLocation,
) *ManifestPathResolver {
	return &ManifestPathResolver{
		internalStore:    internalStore,
		externalStore:    externalStore,
		manifestBasePath: manifestBasePath,
		location:         location,
	}
}

// Resolve returns the store, resolved object key and backend for a manifest
// file path. dirPrefix is the layout directory of the section the path belongs
// to (_data, _delta, _stats, _index or ../lobs).
func (r *ManifestPathResolver) Resolve(filePath, dirPrefix string) (oss.ObjectStore, string, FileBackend, error) {
	trimmed := strings.TrimSpace(filePath)
	if trimmed == "" {
		return nil, "", 0, fmt.Errorf("manifest file path is empty")
	}

	if strings.Contains(trimmed, "://") {
		key, err := ResolveExternalObjectKey(r.location, trimmed)
		if err != nil {
			return nil, "", 0, err
		}
		return r.externalStore, key, FileBackendExternal, nil
	}

	if strings.Contains(trimmed, "ROOT_PATH") {
		key := strings.ReplaceAll(trimmed, "ROOT_PATH", r.location.RootPath)
		return r.externalStore, path.Clean(strings.TrimPrefix(key, "/")), FileBackendExternal, nil
	}

	rel := strings.TrimPrefix(strings.TrimSpace(trimmed), "/")
	key := path.Join(r.manifestBasePath, dirPrefix, rel)
	return r.internalStore, key, FileBackendInternal, nil
}

// ResolveExternalObjectKey resolves an external file reference (absolute URI,
// ROOT_PATH placeholder, or path relative to the external source) into an
// object key used with the external store. Absolute URIs are validated against
// the location according to its form (Milvus-form: host = endpoint, first path
// segment = bucket; AWS-form: host = bucket, full path = key).
func ResolveExternalObjectKey(location ExternalSourceLocation, externalFile string) (string, error) {
	trimmed := strings.TrimSpace(externalFile)
	if trimmed == "" {
		return "", fmt.Errorf("external file path is empty")
	}
	if strings.Contains(trimmed, "ROOT_PATH") {
		return strings.ReplaceAll(trimmed, "ROOT_PATH", location.RootPath), nil
	}
	if strings.Contains(trimmed, "://") {
		u, err := url.Parse(trimmed)
		if err != nil {
			return "", fmt.Errorf("parse external file path: %w", err)
		}
		if location.Form == LocationFormAWS {
			// scheme://bucket/key — host is the bucket, path is the key.
			if u.Host != "" && u.Host != location.Bucket {
				return "", fmt.Errorf("external file host %s does not match external source bucket %s", u.Host, location.Bucket)
			}
			trimmed = strings.TrimPrefix(u.Path, "/")
		} else {
			// scheme://endpoint/bucket/key — host is the endpoint.
			if u.Host != "" && u.Host != location.Address && u.Host != location.Host {
				return "", fmt.Errorf("external file host %s does not match external source endpoint %s", u.Host, location.Address)
			}
			parts := strings.Split(strings.Trim(u.Path, "/"), "/")
			if len(parts) == 0 || parts[0] == "" {
				return "", fmt.Errorf("external file path %s does not include bucket", trimmed)
			}
			if parts[0] != location.Bucket {
				return "", fmt.Errorf("external file bucket %s does not match external source bucket %s", parts[0], location.Bucket)
			}
			trimmed = path.Join(parts[1:]...)
		}
	}
	trimmed = strings.TrimPrefix(trimmed, "/")
	if location.RootPath == "" {
		return path.Clean(trimmed), nil
	}
	if trimmed == location.RootPath || strings.HasPrefix(trimmed, location.RootPath+"/") {
		return path.Clean(trimmed), nil
	}
	return path.Join(location.RootPath, trimmed), nil
}

// ResolveExternalObjectURI resolves a complete URI without applying RootPath.
// Both bucket and endpoint URI forms may refer to the same configured store.
// The minio scheme explicitly selects endpoint form when the host is ambiguous.
func ResolveExternalObjectURI(location ExternalSourceLocation, raw string) (string, error) {
	uri := strings.TrimSpace(raw)
	u, err := url.Parse(uri)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("external file requires a complete object URI")
	}
	if u.User != nil || u.ForceQuery || u.RawQuery != "" || strings.Contains(uri, "#") {
		return "", fmt.Errorf("external object URI must not contain user info, a query, or a fragment")
	}
	endpoint := location.Address
	if endpoint == "" && location.Form == LocationFormMilvus {
		endpoint = location.Host
	}
	matchesEndpoint := u.Host == endpoint
	matchesBucket := u.Host == location.Bucket
	requireEndpoint := strings.EqualFold(u.Scheme, "minio") || !matchesBucket
	if requireEndpoint && !matchesEndpoint {
		return "", fmt.Errorf("external file host does not match the configured endpoint or bucket")
	}
	key := strings.TrimPrefix(u.EscapedPath(), "/")
	ambiguous := false
	if matchesEndpoint {
		bucket, rest, _ := strings.Cut(key, "/")
		bucket, err := url.PathUnescape(bucket)
		bucketMatches := err == nil && bucket == location.Bucket
		if requireEndpoint {
			if !bucketMatches {
				return "", fmt.Errorf("external file bucket does not match the configured bucket")
			}
			key = rest
		} else {
			ambiguous = bucketMatches && rest != ""
		}
	}
	if key == "" {
		return "", fmt.Errorf("external object URI does not include an object key")
	}
	key, err = url.PathUnescape(key)
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(key, "/") || strings.Contains(key, "//") {
		return "", fmt.Errorf("external object key must not contain a leading slash or repeated slashes")
	}
	for segment := range strings.SplitSeq(key, "/") {
		if segment == ".." {
			return "", fmt.Errorf("external object key must not contain a '..' path segment")
		}
	}
	if ambiguous {
		return "", fmt.Errorf("ambiguous external object URI: both bucket and endpoint forms are valid; use minio://endpoint/bucket/key for endpoint form")
	}
	return key, nil
}

package storage

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/autocomplete"
)

type OSSState struct {
	*framework.CmdState

	connectParam oss.MinioClientParam
	store        oss.ObjectStore
	bucket       string
	rootPath     string
	prefix       string
	config       *configs.Config
}

const minioPathSuggester = "minio-path"

func (s *OSSState) SetupCommands() {
	cmd := s.GetCmd()

	s.MergeFunctionCommands(cmd, s)
	s.UpdateState(cmd, s, s.SetupCommands)

	autocomplete.RegisterValueSuggester(minioPathSuggester, autocomplete.ValueSuggestFunc(func(partial string) []string {
		return s.suggestPaths(partial)
	}))
}

// Label overrides default cmd label behavior
// returning OSS[PROVIDER](BUCKET/CURR_DIR)
func (s *OSSState) Label() string {
	return fmt.Sprintf("OSS[%s](%s/%s)", s.connectParam.CloudProvider, s.connectParam.BucketName, s.prefix)
}

func (s *OSSState) Close() {
	autocomplete.UnregisterValueSuggester(minioPathSuggester)
}

func (s *OSSState) RootPath() string {
	return s.rootPath
}

func (s *OSSState) suggestPaths(partial string) []string {
	if s.config != nil {
		value, err := s.config.GetConfig("env", configs.OSSPathCompletion)
		if err == nil {
			if enabled, err := strconv.ParseBool(value); err == nil && !enabled {
				return nil
			}
		}
	}

	var listPrefix string
	var filterPart string
	var resultPrefix string

	switch {
	// List current directory (or root for "/")
	case partial == "" || partial == "/":
		if partial == "/" {
			listPrefix = ""
			resultPrefix = "/"
		} else {
			listPrefix = s.getBase()
			resultPrefix = ""
		}
		filterPart = ""
	// "subdir/" → list inside that directory
	case strings.HasSuffix(partial, "/"):
		resolved := s.resolvePath(partial)
		listPrefix = toListingPrefix(resolved)
		resultPrefix = partial
		filterPart = ""
	// "sub" or "dir/sub" → list parent, filter by base
	default:
		dir := path.Dir(partial)
		filterPart = path.Base(partial)
		if dir == "." {
			listPrefix = s.getBase()
			resultPrefix = ""
		} else {
			resolved := s.resolvePath(dir)
			listPrefix = toListingPrefix(resolved)
			resultPrefix = dir + "/"
		}
	}

	ctx, cancel := s.Ctx()
	defer cancel()

	ch, err := s.store.List(ctx, listPrefix, false)
	if err != nil {
		return nil
	}

	var results []string
	for info := range ch {
		if info.Err != nil {
			continue
		}
		name := strings.TrimPrefix(info.Key, listPrefix)
		name = strings.TrimSuffix(name, "/")
		if filterPart == "" || strings.HasPrefix(name, filterPart) {
			results = append(results, resultPrefix+name)
		}
	}
	return results
}

type ConnectOSSParam struct {
	framework.ParamBase `use:"connect oss" desc:"connect to OSS instance using underling minio client"`
	Bucket              string `name:"bucket" default:"" desc:"bucket name"`
	Address             string `name:"address" default:"127.0.0.1" desc:"minio address to connect"`
	Port                string `name:"port" default:"9000"`
	CloudProvider       string `name:"cloudProvider" default:"aws"`
	Region              string `name:"region" default:""`
	RootPath            string `name:"rootPath" default:""`
	UseIAM              bool   `name:"iam" default:"false" desc:"use IAM mode"`
	IAMEndpoint         string `name:"iamEndpoint" default:"" desc:"IAM endpoint address"`
	AK                  string `name:"ak" default:"" desc:"access key/username"`
	SK                  string `name:"sk" default:"" desc:"secret key/password"`
	UseSSL              bool   `name:"ssl" default:"" desc:"use SSL"`
	SkipBucketCheck     bool   `name:"skipBucketCheck" default:"true"`
}

func ConnectOSS(ctx context.Context, p *ConnectOSSParam, parent *framework.CmdState, config ...*configs.Config) (*OSSState, error) {
	mp := oss.MinioClientParam{
		CloudProvider: p.CloudProvider,
		Region:        p.Region,
		Addr:          p.Address,
		Port:          p.Port,
		AK:            p.AK,
		SK:            p.SK,

		BucketName: p.Bucket,
		RootPath:   p.RootPath,

		UseIAM: p.UseIAM,
		UseSSL: p.UseSSL,
	}
	if p.SkipBucketCheck {
		oss.WithSkipCheckBucket(true)(&mp)
	}

	mClient, err := oss.NewMinioClient(ctx, mp)
	if err != nil {
		return nil, err
	}

	var cfg *configs.Config
	if len(config) > 0 {
		cfg = config[0]
	}
	return &OSSState{
		store:        oss.NewMinioObjectStore(mClient),
		bucket:       p.Bucket,
		rootPath:     mp.RootPath,
		connectParam: mp,
		prefix:       mp.RootPath,
		config:       cfg,
		CmdState:     parent.Spawn(fmt.Sprintf("OSS[%s](%s/%s)", p.CloudProvider, p.Bucket, p.RootPath)),
	}, nil
}

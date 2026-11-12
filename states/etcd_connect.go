package states

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"path"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/txnkv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/kv"
)

const (
	DefaultMetaPath = `meta`
	metaPath        = DefaultMetaPath
)

// ErrNotMilvsuRootPath sample error for non-valid root path.
var ErrNotMilvsuRootPath = errors.New("is not a Milvus RootPath")

func PingMetaStore(ctx context.Context, cli kv.MetaKV, rootPath string, metaPath string) error {
	key := path.Join(rootPath, metaPath, "session/id")
	_, err := cli.Load(ctx, key)
	if err != nil {
		return err
	}
	return nil
}

func GetTiKVClient(cp *ConnectParams) (*txnkv.Client, error) {
	if cp.TiKVUseSSL {
		f := func(conf *config.Config) {
			conf.Security = config.NewSecurity(cp.TiKVTLSCACert, cp.TiKVTLSCert, cp.TiKVTLSKey, []string{})
		}
		config.UpdateGlobal(f)
		return txnkv.NewClient([]string{cp.TiKVAddr})
	}
	return txnkv.NewClient([]string{cp.TiKVAddr})
}

func (app *ApplicationState) ConnectCommand(ctx context.Context, cp *ConnectParams) error {
	if cp.UseTiKV {
		return app.connectTiKV(ctx, cp)
	}
	return app.connectEtcd(ctx, cp)
}

func (app *ApplicationState) connectTiKV(ctx context.Context, cp *ConnectParams) error {
	tikvCli, err := GetTiKVClient(cp)
	if err != nil {
		return err
	}

	cli := kv.NewTiKV(tikvCli)
	kvState := getKVConnectedState(app.core, cli, cp.TiKVAddr, app.config, app.extensions, app.objectStoreProvider)
	if !cp.Dry {
		ctx, cancel := context.WithTimeout(ctx, time.Second*10)
		defer cancel()
		err = PingMetaStore(ctx, cli, cp.RootPath, cp.MetaPath)
		if err != nil {
			if errors.Is(err, ErrNotMilvsuRootPath) {
				if !cp.Force {
					cli.Close()
					fmt.Printf("Connection established, but %s, please check your config or use Dry mode\n", err.Error())
					return err
				}
			} else {
				fmt.Println("cannot connect to tikv with addr:", cp.TiKVAddr, err.Error())
				return err
			}
		}

		fmt.Fprintln(os.Stderr, "Using meta path:", fmt.Sprintf("%s/%s/", cp.RootPath, metaPath))

		// use rootPath as instanceName
		app.SetTagNext(tikvTag, GetInstanceState(app.core, cli, cp.RootPath, cp.MetaPath, kvState, app.config, app.extensions, app.objectStoreProvider))
	} else {
		fmt.Fprintln(os.Stderr, "using dry mode, ignore rootPath and metaPath")
		// rootPath empty fall back to metastore connected state
		app.SetTagNext(tikvTag, kvState)
	}
	return nil
}

func (app *ApplicationState) readEnv(cp *ConnectParams) {
	if cp.EtcdAddr == "127.0.0.1:2379" && os.Getenv("MILVUS_ETCD_ADDR") != "" {
		cp.EtcdAddr = os.Getenv("MILVUS_ETCD_ADDR")
		fmt.Println("using env MILVUS_ETCD_ADDR,", cp.EtcdAddr)
	}
	if cp.RootPath == "by-dev" && os.Getenv("MILVUS_ROOT_PATH") != "" {
		cp.RootPath = os.Getenv("MILVUS_ROOT_PATH")
		cp.rootPathFromEnv = true
		fmt.Println("using env MILVUS_ROOT_PATH,", cp.RootPath)
	}
}

func (app *ApplicationState) connectEtcd(ctx context.Context, cp *ConnectParams) error {
	app.readEnv(cp)
	tls, err := app.getTLSConfig(cp)
	if err != nil {
		return err
	}
	_, _, err = net.SplitHostPort(cp.EtcdAddr)
	if err != nil {
		if strings.Contains(err.Error(), "missing port in address") {
			cp.EtcdAddr += ":2379"
		} else {
			return errors.Wrap(err, "invalid etcd address")
		}
	}

	cfg := clientv3.Config{
		Endpoints:   []string{cp.EtcdAddr},
		DialTimeout: time.Second * 10,
		DialOptions: []grpc.DialOption{
			grpc.WithBlock(),
		},

		Username: cp.ETCDUserName,
		Password: cp.ETCDPassword,

		TLS: tls,
		// disable grpc logging
		Logger: zap.NewNop(),
	}
	etcdCli, err := clientv3.New(cfg)
	if err != nil {
		return errors.Wrap(err, "failed to connect to etcd")
	}

	cli := kv.NewEtcdKV(etcdCli)

	if cp.Auto && !cp.rootPathProvided() {
		candidates, err := findMilvusInstance(ctx, cli)
		if err != nil {
			return err
		}
		switch {
		case len(candidates) == 1:
			cp.RootPath = candidates[0]
		case len(candidates) > 1:
			fmt.Println("multiple possible rootPath find, cannot use auto mode")
		default:
			fmt.Println("failed to find rootPath candidate")
			return nil
		}
	} else if cp.Auto {
		fmt.Fprintln(os.Stderr, "rootPath provided explicitly, skip auto detection")
	}

	kvState := getKVConnectedState(app.core, cli, cp.EtcdAddr, app.config, app.extensions, app.objectStoreProvider)
	if !cp.Dry {
		// ping etcd
		ctx, cancel := context.WithTimeout(ctx, time.Second*10)
		defer cancel()
		if !cp.Force {
			err = PingMetaStore(ctx, cli, cp.RootPath, cp.MetaPath)
			if err != nil {
				fmt.Printf("Connection established, but %s, please check your config or use Dry mode\n", err.Error())
				return err
			}
		}

		fmt.Fprintln(os.Stderr, "Using meta path:", fmt.Sprintf("%s/%s/", cp.RootPath, metaPath))

		// use rootPath as instanceName
		app.SetTagNext(etcdTag, GetInstanceState(app.core, cli, cp.RootPath, cp.MetaPath, kvState, app.config, app.extensions, app.objectStoreProvider))
	} else {
		fmt.Fprintln(os.Stderr, "using dry mode, ignore rootPath and metaPath")
		// rootPath empty fall back to etcd connected state
		app.SetTagNext(etcdTag, kvState)
	}
	return nil
}

type ConnectParams struct {
	framework.ParamBase `use:"connect" desc:"Connect to metastore"`
	EtcdAddr            string `name:"etcd" default:"127.0.0.1:2379" desc:"the etcd endpoint to connect"`
	RootPath            string `name:"rootPath" default:"by-dev" desc:"meta root paht milvus is using"`
	MetaPath            string `name:"metaPath" default:"meta" desc:"meta path prefix"`
	Force               bool   `name:"force" default:"false" desc:"force connect ignoring ping Etcd & rootPath check"`
	Dry                 bool   `name:"dry" default:"false" desc:"dry connect without specifying milvus instance"`
	UseSSL              bool   `name:"use_ssl" default:"false" desc:"enable to use SSL"`
	EnableTLS           bool   `name:"enableTLS" default:"false" desc:"use TLS"`
	RootCA              string `name:"rootCAPem" default:"" desc:"root CA pem file path"`
	ETCDPem             string `name:"etcdCert" default:"" desc:"etcd tls cert file path"`
	ETCDKey             string `name:"etcdKey" default:"" desc:"etcd tls key file path"`
	ETCDUserName        string `name:"etcdUserName" default:"" desc:"etcd credential username"`
	ETCDPassword        string `name:"etcdPassword" default:"" desc:"etcd creidentail password" secret:"true"`
	TLSMinVersion       string `name:"min_version" default:"1.2" desc:"TLS min version"`
	// TiKV Params
	UseTiKV       bool   `name:"use_tikv" default:"false" desc:"enable to use tikv for metastore"`
	TiKVTLSCACert string `name:"tikvCACert" default:"" desc:"tikv root CA pem file path"`
	TiKVTLSCert   string `name:"tikvCert" default:"" desc:"tikv tls cert file path"`
	TiKVTLSKey    string `name:"tikvKey" default:"" desc:"tikv tls key file path"`
	TiKVUseSSL    bool   `name:"tikv_use_ssl" default:"false" desc:"enable to use SSL for tikv"`
	TiKVAddr      string `name:"tikv" default:"127.0.0.1:2389" desc:"the tikv endpoint to connect"`

	Auto bool `name:"auto" default:"false" desc:"auto detect rootPath if possible"`

	// unexported runtime state
	flagSet         *pflag.FlagSet
	rootPathFromEnv bool
}

// SetFlagSet implements framework.FlagSetAware to keep the parsed flagset,
// enabling detection of whether rootPath was explicitly provided by the user.
func (cp *ConnectParams) SetFlagSet(fs *pflag.FlagSet) {
	cp.flagSet = fs
}

// rootPathProvided reports whether the user explicitly provided a rootPath via
// flag or environment variable; when so, auto detection should be skipped to
// avoid scanning the whole etcd keyspace.
func (cp *ConnectParams) rootPathProvided() bool {
	if cp.rootPathFromEnv {
		return true
	}
	return cp.flagSet != nil && cp.flagSet.Changed("rootPath")
}

func (app *ApplicationState) getTLSConfig(cp *ConnectParams) (*tls.Config, error) {
	if !cp.EnableTLS {
		return nil, nil
	}

	var tlsMinVersion uint16
	switch cp.TLSMinVersion {
	case "1.0":
		tlsMinVersion = tls.VersionTLS10
	case "1.1":
		tlsMinVersion = tls.VersionTLS11
	case "1.2":
		tlsMinVersion = tls.VersionTLS12
	case "1.3":
		tlsMinVersion = tls.VersionTLS13
	default:
		return nil, errors.New("invalid min tls version, only 1.0, 1.1, 1.2 and 1.3 is supported")
	}

	rootCertPool, err := x509.SystemCertPool()
	if err != nil {
		return nil, err
	}

	if cp.RootCA != "" {
		bs, err := os.ReadFile(cp.RootCA)
		if err != nil {
			return nil, err
		}

		ok := rootCertPool.AppendCertsFromPEM(bs)
		if !ok {
			return nil, errors.New("Root CA PEM cannot be parsed")
		}
	}

	cert, err := tls.LoadX509KeyPair(cp.ETCDPem, cp.ETCDKey)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load etcd cert/key pair")
	}

	// #nosec G402
	return &tls.Config{
		RootCAs: rootCertPool,
		Certificates: []tls.Certificate{
			cert,
		},
		MinVersion: tlsMinVersion,
	}, nil
}

type kvConnectedState struct {
	*framework.CmdState
	client              kv.MetaKV
	addr                string
	candidates          []string
	config              *configs.Config
	extensions          []Extension
	objectStoreProvider ObjectStoreProvider
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *kvConnectedState) SetupCommands() {
	cmd := s.GetCmd()

	s.UpdateState(cmd, s, s.SetupCommands)
}

// GetKVConnectedState returns kvConnectedState for unknown instance
func GetKVConnectedState(parent *framework.CmdState, cli kv.MetaKV, addr string, config *configs.Config, extensions []Extension, objectStoreProvider ObjectStoreProvider) framework.State {
	state := &kvConnectedState{
		CmdState:            parent.Spawn(fmt.Sprintf("MetaStore(%s)", addr)),
		client:              cli,
		addr:                addr,
		config:              config,
		extensions:          extensions,
		objectStoreProvider: objectStoreProvider,
	}

	return state
}

func getKVConnectedState(parent *framework.CmdState, cli kv.MetaKV, addr string, config *configs.Config, extensions []Extension, objectStoreProvider ObjectStoreProvider) framework.State {
	return GetKVConnectedState(parent, cli, addr, config, extensions, objectStoreProvider)
}

type FindMilvusParam struct {
	framework.ParamBase `use:"find-milvus" desc:"search kvs to find milvus instance"`
}

func (s *kvConnectedState) FindMilvusCommand(ctx context.Context, p *FindMilvusParam) error {
	apps, err := s.client.GetAllRootPath(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("%d candidates found:\n", len(apps))
	for _, app := range apps {
		fmt.Println(app)
	}
	s.candidates = apps
	return nil
}

type UseParam struct {
	framework.ParamBase `use:"use [instance-name]" desc:"use specified milvus instance"`
	instanceName        string
	Force               bool   `name:"force" default:"false" desc:"force connect ignoring ping result"`
	MetaPath            string `name:"metaPath" default:"meta" desc:"meta path prefix"`
}

func (p *UseParam) ParseArgs(args []string) error {
	if len(args) != 1 {
		return errors.New("instance shall be provided")
	}
	p.instanceName = args[0]
	return nil
}

func (s *kvConnectedState) UseCommand(ctx context.Context, p *UseParam) error {
	err := PingMetaStore(ctx, s.client, p.instanceName, p.MetaPath)
	if err != nil {
		if errors.Is(err, ErrNotMilvsuRootPath) {
			if !p.Force {
				fmt.Printf("Connection established, but %s, please check your config or use Dry mode\n", err.Error())
				return err
			}
		} else {
			fmt.Println("failed to ping metastore", err.Error())
			return err
		}
	}

	fmt.Fprintf(os.Stderr, "Using meta path: %s/%s/\n", p.instanceName, p.MetaPath)

	s.SetNext(etcdTag, GetInstanceState(s.CmdState, s.client, p.instanceName, p.MetaPath, s, s.config, s.extensions, nil))
	return nil
}

// findMilvusInstance enumerates possible root paths, skipping the reserved
// woodpecker namespace. Uses the streaming Range API when the server supports
// it and falls back to the legacy scan otherwise.
func findMilvusInstance(ctx context.Context, cli kv.MetaKV) ([]string, error) {
	apps, err := cli.GetAllRootPath(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(apps))
	for _, app := range apps {
		if app != "woodpecker" { // skip woodpecker prefix
			result = append(result, app)
		}
	}
	return result, nil
}

func (s *kvConnectedState) Close() {
	s.client.Close()
}

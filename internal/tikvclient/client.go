// Package tikvclient creates TiKV clients with cancellable initialization.
package tikvclient

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/util"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// New follows txnkv.NewClient's API V1 setup, but supplies a cancellable PD
// client instead of its unconfigurable, 100-attempt constructor. ctx controls
// initialization only; the returned client lives until Close is called.
func New(ctx context.Context, addr string, security config.Security) (_ *txnkv.Client, retErr error) {
	start := time.Now()
	stage := "TLS configuration"
	storeOwnsResources := false
	var rpcErrors initRPCErrors
	defer func() {
		if retErr != nil {
			retErr = fmt.Errorf("connect TiKV via PD %q: %s failed after %s: %w",
				addr, stage, time.Since(start).Round(time.Millisecond),
				errors.Join(ctx.Err(), retErr, rpcErrors.last()))
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	tlsConfig, err := security.ToTLSConfig()
	if err != nil {
		return nil, err
	}

	// The PD context also owns its background workers. Detach it from the
	// command context, forwarding cancellation only while initializing.
	clientCtx, cancelClient := context.WithCancel(context.Background())
	stopCancel := context.AfterFunc(ctx, cancelClient)
	defer func() {
		stopCancel()
		if retErr != nil {
			cancelClient()
		}
	}()

	cfg := config.GetGlobalConfig()
	stage = "PD discovery"
	pdClient, err := pd.NewClientWithContext(clientCtx, []string{addr}, pd.SecurityOption{
		CAPath: security.ClusterSSLCA, CertPath: security.ClusterSSLCert, KeyPath: security.ClusterSSLKey,
	},
		opt.WithMaxErrorRetry(1),
		opt.WithCustomTimeoutOption(time.Duration(cfg.PDClient.PDServerTimeout)*time.Second),
		opt.WithForwardingOption(cfg.EnableForwarding),
		opt.WithGRPCDialOptions(
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:    time.Duration(cfg.TiKVClient.GrpcKeepAliveTime) * time.Second,
				Timeout: cfg.TiKVClient.GetGrpcKeepAliveTimeout(),
			}),
			grpc.WithChainUnaryInterceptor(rpcErrors.intercept),
		),
	)
	if err != nil {
		return nil, err
	}
	pdClient = &ownedPDClient{Client: pdClient, cancel: cancelClient}
	defer func() {
		if retErr != nil && !storeOwnsResources {
			pdClient.Close()
		}
	}()
	codecClient := tikv.NewCodecPDClient(tikv.ModeTxn, util.InterceptedPDClient{Client: pdClient})

	stage = "safe-point client initialization"
	spkv, err := tikv.NewEtcdSafePointKV([]string{addr}, tlsConfig)
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil && !storeOwnsResources {
			spkv.Close()
		}
	}()
	rpcClient := tikv.NewRPCClient(tikv.WithSecurity(security), tikv.WithCodec(codecClient.GetCodec()))
	defer func() {
		if retErr != nil && !storeOwnsResources {
			rpcClient.Close()
		}
	}()

	stage = "timestamp oracle initialization"
	uuid := fmt.Sprintf("tikv-%v", codecClient.GetClusterID(clientCtx))
	store, err := tikv.NewKVStore(uuid, codecClient, spkv, rpcClient)
	if err != nil {
		return nil, err
	}
	storeOwnsResources = true
	if cfg.TxnLocalLatches.Enabled {
		store.EnableTxnLocalLatches(cfg.TxnLocalLatches.Capacity)
	}

	// Stop before returning so canceling the command (or its deadline expiring)
	// cannot tear down a successfully initialized client.
	stopCancel()
	if err := ctx.Err(); err != nil {
		cancelClient()
		store.Close()
		return nil, err
	}
	rpcErrors.stop()
	return &txnkv.Client{KVStore: store}, nil
}

type ownedPDClient struct {
	pd.Client
	cancel context.CancelFunc
	once   sync.Once
}

func (c *ownedPDClient) Close() {
	c.once.Do(func() {
		c.cancel()
		c.Client.Close()
	})
}

// PD's initClusterID discards the RPC error and returns only "failed to get
// cluster id". Retain the actual RPC target and cause until setup finishes.
type initRPCErrors struct {
	mu       sync.Mutex
	finished bool
	err      error
}

func (e *initRPCErrors) intercept(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	err := invoker(ctx, method, req, reply, cc, opts...)
	if err != nil {
		e.mu.Lock()
		if !e.finished {
			e.err = fmt.Errorf("%s at %s: %w", method, cc.Target(), err)
		}
		e.mu.Unlock()
	}
	return err
}

func (e *initRPCErrors) last() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.err
}

func (e *initRPCErrors) stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.finished = true
	e.err = nil
}

package tikvclient

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

// All servers and advertised addresses in these tests are loopback-only.
type testPD struct {
	pdpb.UnimplementedPDServer
	addr         string
	leader       string
	membersErr   error
	blockMembers bool
	blockTSO     bool
	membersCalls atomic.Int64
	tsoCalls     atomic.Int64
	connections  atomic.Int64
	logical      atomic.Int64
}

func (s *testPD) GetMembers(ctx context.Context, _ *pdpb.GetMembersRequest) (*pdpb.GetMembersResponse, error) {
	s.membersCalls.Add(1)
	if s.blockMembers {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if s.membersErr != nil {
		return nil, s.membersErr
	}
	addr := s.addr
	if s.leader != "" {
		addr = s.leader
	}
	leader := &pdpb.Member{MemberId: 1, ClientUrls: []string{"http://" + addr}}
	return &pdpb.GetMembersResponse{
		Header: &pdpb.ResponseHeader{ClusterId: 42}, Members: []*pdpb.Member{leader}, Leader: leader,
	}, nil
}

func (s *testPD) GetClusterInfo(context.Context, *pdpb.GetClusterInfoRequest) (*pdpb.GetClusterInfoResponse, error) {
	return &pdpb.GetClusterInfoResponse{
		Header: &pdpb.ResponseHeader{ClusterId: 42}, ServiceModes: []pdpb.ServiceMode{pdpb.ServiceMode_PD_SVC_MODE},
	}, nil
}

func (s *testPD) Tso(stream pdpb.PD_TsoServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		s.tsoCalls.Add(1)
		if s.blockTSO {
			<-stream.Context().Done()
			return stream.Context().Err()
		}
		err = stream.Send(&pdpb.TsoResponse{
			Header: &pdpb.ResponseHeader{ClusterId: 42}, Count: req.Count,
			Timestamp: &pdpb.Timestamp{Physical: time.Now().UnixMilli(), Logical: s.logical.Add(int64(req.Count))},
		})
		if err != nil {
			return err
		}
	}
}

func (s *testPD) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context   { return ctx }
func (s *testPD) HandleRPC(context.Context, stats.RPCStats)                         {}
func (s *testPD) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }
func (s *testPD) HandleConn(_ context.Context, event stats.ConnStats) {
	switch event.(type) {
	case *stats.ConnBegin:
		s.connections.Add(1)
	case *stats.ConnEnd:
		s.connections.Add(-1)
	}
}

func startTestPD(t *testing.T, pd *testPD, opts ...grpc.ServerOption) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	pd.addr = listener.Addr().String()
	server := grpc.NewServer(append(opts, grpc.StatsHandler(pd))...)
	pdpb.RegisterPDServer(server, pd)
	healthpb.RegisterHealthServer(server, health.NewServer())
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	return pd.addr
}

func requireDisconnected(t *testing.T, pd *testPD) {
	t.Helper()
	require.Eventually(t, func() bool { return pd.connections.Load() == 0 }, time.Second, 10*time.Millisecond)
}

func TestNewPDDiscoveryFailure(t *testing.T) {
	pd := &testPD{membersErr: status.Error(codes.Unavailable, "simulated PD unavailable")}
	addr := startTestPD(t, pd)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	client, err := New(ctx, addr, config.Security{})
	require.Error(t, err)
	require.Nil(t, client)
	require.Less(t, time.Since(start), 3*time.Second)
	require.EqualValues(t, 1, pd.membersCalls.Load())
	require.ErrorContains(t, err, "PD discovery")
	require.ErrorContains(t, err, "simulated PD unavailable")
	require.ErrorContains(t, err, addr)
	requireDisconnected(t, pd)
}

func TestNewPDDeadline(t *testing.T) {
	pd := &testPD{blockMembers: true}
	addr := startTestPD(t, pd)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	client, err := New(ctx, addr, config.Security{})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, client)
	require.Less(t, time.Since(start), time.Second)
	require.EqualValues(t, 1, pd.membersCalls.Load())
	require.ErrorContains(t, err, "GetMembers")
	requireDisconnected(t, pd)
}

func TestNewCanceled(t *testing.T) {
	pd := &testPD{}
	addr := startTestPD(t, pd)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client, err := New(ctx, addr, config.Security{})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, client)
	require.Zero(t, pd.membersCalls.Load())
}

func TestNewCancellationDuringDiscovery(t *testing.T) {
	pd := &testPD{blockMembers: true}
	addr := startTestPD(t, pd)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	timer := time.AfterFunc(100*time.Millisecond, cancel)
	defer timer.Stop()
	start := time.Now()
	client, err := New(ctx, addr, config.Security{})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, client)
	require.Less(t, time.Since(start), time.Second)
	requireDisconnected(t, pd)
}

func TestNewAdvertisedLeaderUnreachable(t *testing.T) {
	pd := &testPD{leader: "127.0.0.1:0"}
	addr := startTestPD(t, pd)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	client, err := New(ctx, addr, config.Security{})
	require.Error(t, err)
	require.Nil(t, client)
	require.Less(t, time.Since(start), 3*time.Second)
	require.ErrorContains(t, err, "GetClusterInfo")
	require.ErrorContains(t, err, pd.leader)
	requireDisconnected(t, pd)
}

func TestNewTimestampOracleDeadline(t *testing.T) {
	pd := &testPD{blockTSO: true}
	addr := startTestPD(t, pd)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	client, err := New(ctx, addr, config.Security{})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, client)
	require.Less(t, time.Since(start), 2*time.Second)
	require.Positive(t, pd.tsoCalls.Load())
	require.ErrorContains(t, err, "timestamp oracle initialization")
	requireDisconnected(t, pd)
}

func TestNewClientSurvivesInitializationContext(t *testing.T) {
	for _, expire := range []bool{false, true} {
		t.Run(map[bool]string{false: "canceled", true: "expired"}[expire], func(t *testing.T) {
			pd := &testPD{}
			addr := startTestPD(t, pd)
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			client, err := New(ctx, addr, config.Security{})
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, client.Close())
				requireDisconnected(t, pd)
			})
			require.EqualValues(t, 42, client.GetClusterID())
			if !expire {
				cancel()
			}
			<-ctx.Done()
			requestCtx, requestCancel := context.WithTimeout(context.Background(), time.Second)
			defer requestCancel()
			ts, err := client.GetTimestamp(requestCtx)
			require.NoError(t, err)
			require.Positive(t, ts)
		})
	}
}

func TestNewTLSClient(t *testing.T) {
	// Reuse Go's loopback test certificate for the gRPC PD server.
	tlsServer := httptest.NewTLSServer(http.NotFoundHandler())
	tlsConfig := tlsServer.TLS.Clone()
	tlsServer.Close()
	tlsConfig.NextProtos = []string{"h2"}
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{
		Type: "CERTIFICATE", Bytes: tlsConfig.Certificates[0].Certificate[0],
	}), 0o600))
	keyPath := filepath.Join(t.TempDir(), "key.pem")
	keyDER, err := x509.MarshalPKCS8PrivateKey(tlsConfig.Certificates[0].PrivateKey)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{
		Type: "PRIVATE KEY", Bytes: keyDER,
	}), 0o600))
	pd := &testPD{}
	addr := startTestPD(t, pd, grpc.Creds(credentials.NewTLS(tlsConfig)))
	before := config.GetGlobalConfig().Security
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client, err := New(ctx, addr, config.Security{ClusterSSLCA: caPath, ClusterSSLCert: caPath, ClusterSSLKey: keyPath})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, client.Close())
		requireDisconnected(t, pd)
	})
	require.Positive(t, pd.tsoCalls.Load())
	require.Equal(t, before, config.GetGlobalConfig().Security)
}

func TestNewInvalidTLS(t *testing.T) {
	before := config.GetGlobalConfig().Security
	client, err := New(context.Background(), "127.0.0.1:0", config.Security{ClusterSSLCA: "/nonexistent/birdwatcher-test-ca.pem"})
	require.Error(t, err)
	require.Nil(t, client)
	require.ErrorContains(t, err, "TLS configuration")
	require.Equal(t, before, config.GetGlobalConfig().Security)
}

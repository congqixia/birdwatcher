package states

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/birdwatcher/common"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/commonpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/indexpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/milvuspb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/querypb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/rootcoordpb"
	commonpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/commonpb"
	internalpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/internalpb"
	etcdcommon "github.com/milvus-io/birdwatcher/states/etcd/common"
)

func getSessionTypes() []string {
	return []string{
		"datacoord",
		"datanode",
		"indexcoord",
		"indexnode",
		"querycoord",
		"querynode",
		"rootcoord",
	}
}

func getVisitCmd(state common.State, cli clientv3.KV, basePath string) *cobra.Command {
	callCmd := &cobra.Command{
		Use:   "visit",
		Short: "enter state that could visit some service of component",
	}

	callCmd.AddCommand(
		getVisitSessionCmds(state, cli, basePath)...,
	)

	return callCmd
}

func setNextState(sessionType string, conn *grpc.ClientConn, statePtr *common.State, session *models.Session) {
	state := *statePtr
	switch sessionType {
	case "datacoord":
		client := datapb.NewDataCoordClient(conn)
		state.SetNext(getDataCoordState(client, conn, state, session))
	case "datanode":
		client := datapb.NewDataNodeClient(conn)
		state.SetNext(getDataNodeState(client, conn, state, session))
	case "indexcoord":
		client := indexpb.NewIndexCoordClient(conn)
		state.SetNext(getIndexCoordState(client, conn, state, session))
	case "indexnode":
		client := indexpb.NewIndexNodeClient(conn)
		state.SetNext(getIndexNodeState(client, conn, state, session))
	case "querycoord":
		client := querypb.NewQueryCoordClient(conn)
		state.SetNext(getQueryCoordState(client, conn, state, session))
	case "querynode":
		client := querypb.NewQueryNodeClient(conn)
		state.SetNext(getQueryNodeState(client, conn, state, session))
	case "rootcoord":
		client := rootcoordpb.NewRootCoordClient(conn)
		state.SetNext(getRootCoordState(client, conn, state, session))
	}
}

func getSessionConnect(cli clientv3.KV, basePath string, id int64, sessionType string) (session *models.Session, conn *grpc.ClientConn, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sessions, err := etcdcommon.ListSessions(ctx, cli, basePath)
	if err != nil {
		fmt.Println("failed to list session, err:", err.Error())
		return nil, nil, err
	}

	for _, session := range sessions {
		if id == session.ServerID && session.ServerName == sessionType {
			opts := []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithBlock(),
			}

			conn, err = grpc.DialContext(context.Background(), session.Address, opts...)
			if err != nil {
				fmt.Printf("failed to connect to proxy(%d) addr: %s, err: %s\n", session.ServerID, session.Address, err.Error())
			}
			return session, conn, err
		}
	}

	fmt.Printf("%s id:%d not found\n", sessionType, id)
	return nil, nil, errors.New("invalid id")
}

func getVisitSessionCmds(state common.State, cli clientv3.KV, basePath string) []*cobra.Command {
	sessionCmds := make([]*cobra.Command, 0, len(getSessionTypes()))
	sessionTypes := getSessionTypes()

	RunFuncFactory := func(sessionType string) func(cmd *cobra.Command, args []string) {
		return func(cmd *cobra.Command, args []string) {
			if len(args) < 1 {
				cmd.Usage()
				return
			}
			id, err := strconv.ParseInt(args[0], 10, 64)
			if err != nil {
				cmd.Usage()
				return
			}
			addr, err := cmd.Flags().GetString("addr")
			if err != nil {
				cmd.Usage()
				return
			}
			sType, err := cmd.Flags().GetString("sessionType")
			if err != nil {
				cmd.Usage()
				return
			}
			if addr != "" && sType != "" {
				opts := []grpc.DialOption{
					grpc.WithTransportCredentials(insecure.NewCredentials()),
					grpc.WithBlock(),
				}

				conn, err := grpc.DialContext(context.Background(), addr, opts...)
				if err != nil {
					fmt.Println(err.Error())
					return
				}
				setNextState(sessionType, conn, &state, &models.Session{
					Address: addr,
				})
				return
			}
			session, conn, err := getSessionConnect(cli, basePath, id, sessionType)
			if err != nil {
				return
			}
			setNextState(sessionType, conn, &state, session)
		}
	}

	for i := 0; i < len(sessionTypes); i++ {
		callCmd := &cobra.Command{
			Use:   sessionTypes[i] + " {serverId}",
			Short: "component of " + sessionTypes[i] + "s",
			Run:   RunFuncFactory(sessionTypes[i]),
		}
		callCmd.Flags().String("addr", "", "manual specified grpc addr")
		callCmd.Flags().String("sessionType", "", "")
		sessionCmds = append(sessionCmds, callCmd)
	}
	return sessionCmds
}

type configurationSource interface {
	ShowConfigurations(context.Context, *internalpbv2.ShowConfigurationsRequest, ...grpc.CallOption) (*internalpbv2.ShowConfigurationsResponse, error)
}

type metricsSource interface {
	GetMetrics(context.Context, *milvuspb.GetMetricsRequest, ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error)
}

func getMetrics(ctx context.Context, client metricsSource) (string, error) {
	req := &milvuspb.GetMetricsRequest{
		Base:    &commonpb.MsgBase{},
		Request: `{"metric_type": "system_info"}`,
	}
	resp, err := client.GetMetrics(ctx, req)
	return resp.GetResponse(), err
}

func getConfiguration(ctx context.Context, client configurationSource, id int64) ([]*commonpbv2.KeyValuePair, error) {
	resp, err := client.ShowConfigurations(ctx, &internalpbv2.ShowConfigurationsRequest{
		Base: &commonpbv2.MsgBase{
			SourceID: -1,
			TargetID: id,
		},
	})
	return resp.GetConfiguations(), err
}

func getMetricsCmd(client metricsSource) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "metrics",
		Short:   "show the metrics provided by current server",
		Aliases: []string{"GetMetrics"},
		Run: func(cmd *cobra.Command, args []string) {
			resp, err := client.GetMetrics(context.Background(), &milvuspb.GetMetricsRequest{
				Base:    &commonpb.MsgBase{},
				Request: `{"metric_type": "system_info"}`,
			})
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			fmt.Printf("Metrics: %#v\n", resp.Response)
		},
	}

	return cmd
}

func getConfigurationCmd(client configurationSource, id int64) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "configuration",
		Short:   "call ShowConfigurations for config inspection",
		Aliases: []string{"GetConfigurations", "configurations"},
		Run: func(cmd *cobra.Command, args []string) {
			prefix, err := cmd.Flags().GetString("prefix")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			resp, err := client.ShowConfigurations(context.Background(), &internalpbv2.ShowConfigurationsRequest{
				Base: &commonpbv2.MsgBase{
					SourceID: -1,
					TargetID: id,
				},
			})
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			prefix = strings.ToLower(prefix)
			for _, item := range resp.GetConfiguations() {
				if strings.HasPrefix(item.GetKey(), prefix) {
					fmt.Printf("Key: %s, Value: %s\n", item.Key, item.Value)
				}
			}
		},
	}

	cmd.Flags().String("prefix", "", "the configuration prefix to show")

	return cmd
}

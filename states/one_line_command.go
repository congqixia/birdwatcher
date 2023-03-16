package states

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func ExecOLC(cmd string) {
	// TODO parse cmd
	// use as instance info for now
	result, err := readInstance(cmd)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	name := fmt.Sprintf("%s_probe_query_failure", time.Now().Format("2006-01-02_15-04-05"))
	file, err := os.OpenFile(name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer file.Close()

	for _, instance := range result {
		probeInstance(instance, file)
	}
}

func probeInstance(instance InstanceInfo, w *os.File) {
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{instance.Addr},
		DialTimeout: time.Second * 10,

		// disable grpc logging
		Logger: zap.NewNop(),
	})
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	rootPath := instance.RootPath
	// ping etcd
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	err = pingEtcd(ctx, etcdCli, rootPath, metaPath)
	if err != nil {
		if errors.Is(err, ErrNotMilvsuRootPath) {
			etcdCli.Close()
			fmt.Printf("Connection established, but %s, please check your config or use Dry mode\n", err.Error())
			return
		}
		fmt.Println("cannot connect to etcd with addr:", instance.Addr, err.Error())
		return
	}

	fmt.Println("Using meta path:", fmt.Sprintf("%s/%s/", rootPath, metaPath))
	basePath := path.Join(rootPath, metaPath)

	probeQuery(context.Background(), etcdCli, basePath, func() {
		w.WriteString(fmt.Sprintf("Failure, addr: %s, rootPath %s\n", instance.Addr, instance.RootPath))
	})
}

type InstanceInfo struct {
	Addr     string
	RootPath string
}

func readInstance(fp string) ([]InstanceInfo, error) {
	file, err := os.Open(fp)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var result []InstanceInfo
	// optionally, resize scanner's capacity for lines over 64K, see next example
	for scanner.Scan() {
		raw := scanner.Text()
		parts := strings.Split(raw, "/")
		if len(parts) == 2 {
			result = append(result, InstanceInfo{
				Addr:     parts[0],
				RootPath: parts[1],
			})
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

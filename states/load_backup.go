package states

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
)

func getLoadBackupCmd(state State, config *configs.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "load-backup [file]",
		Short: "load etcd backup file as env",
		Run: func(cmd *cobra.Command, args []string) {
			useWorkspace, err := cmd.Flags().GetBool("use-workspace")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			workspaceName, err := cmd.Flags().GetString("workspace-name")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			if len(args) == 0 {
				fmt.Println("No backup file provided.")
				return
			}
			if len(args) > 1 {
				fmt.Println("only one backup file is allowed")
				return
			}

			arg := args[0]
			f, err := openBackupFile(arg)
			if err != nil {
				return
			}
			defer f.Close()

			r, err := gzip.NewReader(f)
			if err != nil {
				fmt.Println("failed to open gzip reader, err:", err.Error())
				return
			}
			defer r.Close()

			rd := bufio.NewReader(r)
			var header models.BackupHeader
			err = readFixLengthHeader(rd, &header)
			if err != nil {
				fmt.Println("failed to load backup header", err.Error())
				return
			}

			if useWorkspace {
				if workspaceName == "" {
					fileName := path.Base(arg)
					workspaceName = fileName
				}
				workspaceName = createWorkspaceFolder(config, workspaceName)
			}

			server, err := startEmbedEtcdServer(workspaceName, useWorkspace)
			if err != nil {
				fmt.Println("failed to start embed etcd server:", err.Error())
				return
			}
			fmt.Println("using data dir:", server.Config().Dir)
			// TODO
			nextState := getEmbedEtcdInstanceV2(server)
			switch header.Version {
			case 1:
				fmt.Printf("Found backup version: %d, instance name :%s\n", header.Version, header.Instance)
				err = restoreFromV1File(nextState.client, rd, &header)
				if err != nil {
					fmt.Println("failed to restore v1 backup file", err.Error())
					nextState.Close()
					return
				}
				nextState.SetInstance(header.Instance)
			case 2:
				err = restoreV2File(rd, nextState)
				if err != nil {
					fmt.Println("failed to restore v2 backup file", err.Error())
					nextState.Close()
					return
				}
			default:
				fmt.Printf("backup version %d not supported\n", header.Version)
				nextState.Close()
				return
			}
			err = nextState.setupWorkDir(server.Config().Dir)
			if err != nil {
				fmt.Println("failed to setup workspace for backup file", err.Error())
				return
			}

			state.SetNext(nextState)
		},
	}

	cmd.Flags().Bool("use-workspace", false, "load backup into workspace")
	cmd.Flags().String("workspace-name", "", "workspace name if used")
	return cmd
}

func openBackupFile(arg string) (*os.File, error) {
	if strings.Contains(arg, "~") {
		var err error
		arg, err = homedir.Expand(arg)
		if err != nil {
			fmt.Println("path contains tilde, but cannot find home folder", err.Error())
			return nil, err
		}
	}
	err := testFile(arg)
	if err != nil {
		fmt.Println("backup file not valid:", err.Error())
		return nil, err
	}

	f, err := os.Open(arg)
	if err != nil {
		fmt.Printf("failed to open backup file %s, err: %s\n", arg, err.Error())
		return nil, err
	}
	return f, nil
}

func createWorkspaceFolder(config *configs.Config, workspaceName string) string {
	_, err := checkIsDirOrCreate(config.WorkspacePath)
	if err != nil {
		return ""
	}

	workPath := path.Join(config.WorkspacePath, workspaceName)
	preExist, err := checkIsDirOrCreate(workPath)
	if err != nil {
		return ""
	}
	if preExist {
		fmt.Printf("%s already exists!\n", workPath)
		return ""
	}
	return workPath
}

func checkIsDirOrCreate(path string) (bool, error) {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		err = os.Mkdir(path, os.ModePerm)
		if err != nil {
			fmt.Println("failed to create workspace folder", err.Error())
			return false, err
		}
		return false, nil
	}
	if !info.IsDir() {
		return true, fmt.Errorf("%s is a folder", path)
	}
	return true, nil
}

// testFile check file path exists and has access
func testFile(file string) error {
	fi, err := os.Stat(file)
	if err != nil {
		return err
	}
	// not support iterate all possible file under directory for now
	if fi.IsDir() {
		return errors.Wrap(ErrPathIsDir, file)
	}
	return nil
}

func testFolder(folder string) error {
	fi, err := os.Stat(folder)
	if err != nil {
		return err
	}

	if !fi.IsDir() {
		return errors.Newf("path is not dir %s", folder)
	}
	return nil
}

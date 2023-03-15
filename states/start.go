package states

import (
	"fmt"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/models"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/spf13/cobra"
)

// Start returns the first state - offline.
func Start() State {
	root := &cobra.Command{
		Use:   "",
		Short: "",
	}

	state := &cmdState{
		label:   "Offline",
		rootCmd: root,
	}
	app := &ApplicationState{
		State: state,
	}

	var err error
	app.config, err = configs.NewConfig(".bw_config")

	if err != nil {
		// run by default, just printing warning.
		fmt.Println("[WARN] load config file failed", err.Error())
	}

	etcdversion.SetVersion(models.GTEVersion2_2)

	root.AddCommand(
		// connect
		getConnectCommand(state),
		// load-backup
		getLoadBackupCmd(state, app.config),
		// open-workspace
		getOpenWorkspaceCmd(state, app.config),
		// pulsarctl
		getPulsarctlCmd(state),
		//
		GetParseIndexParamCmd(),
		// exit
		getExitCmd(state))

	root.AddCommand(getGlobalUtilCommands()...)

	return app
}

// ApplicationState application background state.
// used for state switch/merging.
type ApplicationState struct {
	// current state
	State

	// config stores configuration items
	config *configs.Config
}

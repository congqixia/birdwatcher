package states

import (
	"context"
	"fmt"
	"strings"

	"github.com/samber/lo"
	"github.com/spf13/cobra"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/milvusctl"
	"github.com/milvus-io/birdwatcher/states/storage"
)

// ApplicationState application background state.
// used for state switch/merging.
type ApplicationState struct {
	// current state
	states map[string]framework.State

	// root *cobra.Command
	core *framework.CmdState

	// config stores configuration items
	config *configs.Config

	extensions          []Extension
	objectStoreProvider ObjectStoreProvider
}

func (app *ApplicationState) Ctx() (context.Context, context.CancelFunc) {
	return app.core.Ctx()
}

func (app *ApplicationState) Core() *framework.CmdState {
	return app.core
}

func (app *ApplicationState) Config() *configs.Config {
	return app.config
}

func (app *ApplicationState) Extensions() []Extension {
	return app.extensions
}

func (app *ApplicationState) State() framework.State {
	return app
}

func (app *ApplicationState) Label() string {
	if len(app.states) == 0 {
		return "Offline"
	}
	builder := &strings.Builder{}

	for _, name := range app.listStates() {
		builder.WriteString(app.states[name].Label())
	}
	return builder.String()
}

func (app *ApplicationState) Process(cmd string) (framework.State, error) {
	app.config.Log("[INFO] begin to process command", framework.RedactCommand(cmd))
	app.core.Process(cmd)
	// perform sub state transfer
	for key, state := range app.states {
		tag, next := state.NextState()
		if next != nil {
			app.config.Log("[DEBUG] set next", key, next.Label())
			state.SetNext(key, nil)
			app.states[tag] = next
		}
	}

	return app, nil
}

func (app *ApplicationState) Close() {
	for _, state := range app.states {
		state.Close()
	}
}

func (app *ApplicationState) SetNext(tag string, state framework.State) {
	app.config.Log("[WARNING] SetNext called for ApplicationState, which is not expected.")
}

func (app *ApplicationState) NextState() (string, framework.State) {
	return "core", app
}

func (app *ApplicationState) SetTagNext(tag string, state framework.State) {
	app.states[tag] = state
}

func (app *ApplicationState) Suggestions(input string) map[string]string {
	result := make(map[string]string)
	states := append(lo.MapToSlice(app.states, func(_ string, state framework.State) framework.State {
		return state
	}), app.core)
	for _, state := range states {
		for k, v := range state.Suggestions(input) {
			result[k] = v
		}
	}
	return result
}

// SetupCommands implments framework.State.
// initialize or reset command after execution.
func (app *ApplicationState) SetupCommands() {
	app.config.Log("[INFO] app state setup commands")
	cmd := app.core.GetCmd()

	app.core.UpdateState(cmd, app, app.SetupCommands)
	app.objectStoreProvider = resolveObjectStoreProvider(app.objectStoreProvider, app.extensions)
	for _, ext := range app.extensions {
		provider, ok := ext.(ApplicationFunctionCommandProvider)
		if !ok {
			continue
		}
		for _, receiver := range provider.ApplicationFunctionCommands(app) {
			app.core.MergeFunctionCommandsFrom(cmd, app, receiver)
		}
	}
	for _, state := range app.states {
		state.SetupCommands()
	}
}

func (app *ApplicationState) IsEnding() bool {
	for _, state := range app.states {
		if state.IsEnding() {
			return true
		}
	}
	return false
}

func (app *ApplicationState) ConnectMinioCommand(ctx context.Context, p *storage.ConnectOSSParam) error {
	state, err := storage.ConnectOSS(ctx, p, app.core, app.config)
	if err != nil {
		return err
	}

	app.SetTagNext(ossTag, state)
	return nil
}

func (app *ApplicationState) ConnectMilvusCommand(ctx context.Context, p *milvusctl.ConnectMilvusParam) error {
	state, err := milvusctl.ConnectMilusctl(ctx, p, app.core)
	if err != nil {
		return err
	}

	app.SetTagNext("milvusctl", state)
	return nil
}

type exitParam struct {
	framework.ParamBase `use:"exit" desc:"Close this CLI tool"`
}

func (app *ApplicationState) ExitCommand(ctx context.Context, _ *exitParam) {
	app.SetTagNext("exit", &exitState{})
}

type debugParam struct {
	framework.ParamBase `use:"debug commands" desc:"debug current command tree"`
}

func (app *ApplicationState) DebugCommand(ctx context.Context, p *debugParam) {
	for _, cmd := range app.core.RootCmd.Commands() {
		app.printCommands(cmd, 0)
	}
}

func (app *ApplicationState) printCommands(cmd *cobra.Command, level int) {
	for i := 0; i < level; i++ {
		fmt.Print("\t")
	}
	fmt.Println(cmd.Use)
	for _, subCmd := range cmd.Commands() {
		app.printCommands(subCmd, level+1)
	}
}

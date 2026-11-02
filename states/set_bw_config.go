package states

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
)

var validator map[string]func(string, string) error

func init() {
	validator = make(map[string]func(string, string) error)
	validator["PAGER"] = func(key string, value string) error {
		if value == "" {
			return nil
		}
		_, err := exec.LookPath(value) // check if the command exists
		return err
	}
	validator[configs.OSSPathCompletion] = func(key string, value string) error {
		if value == "" {
			return nil
		}
		_, err := strconv.ParseBool(value)
		return err
	}
}

type SetConfigParam struct {
	framework.ParamBase `use:"set bw-config" desc:"set birdwatcher config"`
	Key                 string `name:"key" default:""`
	Value               string `name:"value" default:""`
	Source              string `name:"source" default:"env" desc:"config source, default is env"`
}

func (app *ApplicationState) SetEnvCommand(ctx context.Context, p *SetConfigParam) error {
	if validator, ok := validator[p.Key]; ok {
		if err := validator(p.Key, p.Value); err != nil {
			return fmt.Errorf("invalid key-value %s-%s: %w", p.Key, p.Value, err)
		}
	}
	return app.config.SetConfig(p.Source, p.Key, p.Value)
}

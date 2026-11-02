package configs

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	configFileName   = `birdwatcher.yaml`
	defaultWorkspace = `bw_workspace`
	// OSSPathCompletion controls remote path suggestions for the OSS cd command.
	OSSPathCompletion = "OSS_PATH_COMPLETION"
)

var (
	errConfigPathNotExist = errors.New("config path not exist")
	errConfigPathIsFile   = errors.New("config path is file")

	ErrConfigNotFound       = errors.New("config not found")
	ErrConfigSourceNotFound = errors.New("config source not found")
)

// Config stores birdwatcher config items.
type Config struct {
	// birdwatcher configuration folder path
	// default $PWD/.bw_config
	ConfigPath string `yaml:"-"`
	// backup workspace path, default $PWD/bw_workspace
	WorkspacePath string `yaml:"WorkspacePath"`
	// output format: default, plain, json, table, line
	OutputFormat string `yaml:"OutputFormat"`

	logger *log.Logger

	sources map[string]ConfigSource
}

func (c *Config) SetLogger(logger *log.Logger) {
	c.logger = logger
}

func (c *Config) Log(v ...any) {
	if c.logger != nil {
		c.logger.Println(v...)
	}
}

func (c *Config) SetConfig(source, key, value string) error {
	cs, ok := c.sources[strings.ToLower(source)]
	if !ok {
		return ErrConfigSourceNotFound
	}
	return cs.Set(key, value)
}

func (c *Config) GetConfig(source, key string) (string, error) {
	cs, ok := c.sources[strings.ToLower(source)]
	if !ok {
		return "", ErrConfigSourceNotFound
	}
	return cs.Get(key)
}

func (c *Config) load() error {
	err := c.checkConfigPath()
	if err != nil {
		return err
	}

	f, err := os.Open(c.getConfigPath())
	if err != nil {
		return err
	}
	defer f.Close()
	bs, err := io.ReadAll(f)
	if err != nil {
		return err
	}

	return yaml.Unmarshal(bs, c)
}

func (c *Config) getConfigPath() string {
	return path.Join(c.ConfigPath, configFileName)
}

// checkConfigPath exists and is a directory.
func (c *Config) checkConfigPath() error {
	info, err := os.Stat(c.ConfigPath)
	if err != nil {
		// not exist, return specified type to handle
		if os.IsNotExist(err) {
			return errConfigPathNotExist
		}
		return err
	}
	if !info.IsDir() {
		fmt.Printf("%s is not a directory\n", c.ConfigPath)
		return fmt.Errorf("%w(%s)", errConfigPathIsFile, configFileName)
	}

	return nil
}

func (c *Config) createDefault() error {
	err := os.MkdirAll(c.ConfigPath, os.ModePerm)
	if err != nil {
		return err
	}

	file, err := os.Create(c.getConfigPath())
	if err != nil {
		return err
	}
	defer file.Close()

	// setup default value
	c.WorkspacePath = defaultWorkspace

	bs, err := yaml.Marshal(c)
	if err != nil {
		fmt.Println("failed to marshal config", err.Error())
		return err
	}

	file.Write(bs)
	return nil
}

func (c *Config) setupWorkspaceFolder() {
	// try best to setup
	err := os.MkdirAll(c.WorkspacePath, os.ModePerm)
	if err != nil {
		fmt.Println("failed to set workspace folder", err.Error())
	}
}

func (c *Config) setupConfigSources() {
	c.sources = make(map[string]ConfigSource)

	// add env config source
	c.sources["env"] = &envConfigSource{}
}

func NewConfig(configPath string) (*Config, error) {
	config := &Config{
		ConfigPath: configPath,
	}
	err := config.load()
	// config path not exist, may first time to run
	if errors.Is(err, errConfigPathNotExist) {
		err = config.createDefault()
	}

	config.setupWorkspaceFolder()

	config.setupConfigSources()

	return config, err
}

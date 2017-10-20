package edge

import (
	"github.com/BurntSushi/toml"
	"github.com/streamsets/datacollector-edge/container/dpm"
	"github.com/streamsets/datacollector-edge/container/execution"
	"github.com/streamsets/datacollector-edge/container/http"
	"github.com/streamsets/datacollector-edge/container/process"
	"log"
)

// Config represents the configuration format for the Data Collector Edge binary.
type Config struct {
	Execution execution.Config
	Http      http.Config
	DPM       dpm.Config
	Process   process.Config
}

// NewConfig returns a new Config with default settings.
func NewConfig() *Config {
	c := &Config{}
	c.Execution = execution.NewConfig()
	c.Http = http.NewConfig()
	c.DPM = dpm.NewConfig()
	c.Process = process.NewConfig()
	return c
}

// FromTomlFile loads the config from a TOML file.
func (c *Config) FromTomlFile(fPath string) error {
	if _, err := toml.DecodeFile(fPath, c); err != nil {
		log.Println(err)
		return err
	}
	return nil
}

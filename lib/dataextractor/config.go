package dataextractor

import (
	"github.com/streamsets/dataextractor/lib/http"
	"io/ioutil"
	"github.com/BurntSushi/toml"
)

// Config represents the configuration format for the StreamSets Data Extractor binary.
type Config struct {
	Http http.Config

}

// NewConfig returns a new Config with default settings.
func NewConfig() *Config {
	c := &Config{}
	c.Http = http.NewConfig()
	return c
}

// FromTomlFile loads the config from a TOML file.
func (c *Config) FromTomlFile(fPath string) error {
	bs, err := ioutil.ReadFile(fPath)
	if err != nil {
		return err
	}
	return c.FromToml(string(bs))
}

// FromToml loads the config from TOML.
func (c *Config) FromToml(input string) error {
	_, err := toml.Decode(input, c)
	return err
}

// Copyright 2018 StreamSets Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package edge

import (
	"github.com/BurntSushi/toml"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/container/controlhub"
	"github.com/streamsets/datacollector-edge/container/execution"
	"github.com/streamsets/datacollector-edge/container/http"
	"github.com/streamsets/datacollector-edge/container/process"
	"github.com/streamsets/datacollector-edge/container/util"
	"os"
)

// Config represents the configuration format for the Data Collector Edge binary.
type Config struct {
	LogDir    string `toml:"log-dir"`
	Execution execution.Config
	Http      http.Config
	SCH       controlhub.Config
	Process   process.Config
}

// NewConfig returns a new Config with default settings.
func NewConfig() *Config {
	c := &Config{}
	c.Execution = execution.NewConfig()
	c.Http = http.NewConfig()
	c.SCH = controlhub.NewConfig()
	c.Process = process.NewConfig()
	return c
}

// FromTomlFile loads the config from a TOML file.
func (c *Config) FromTomlFile(fPath string) error {
	if _, err := toml.DecodeFile(fPath, c); err != nil {
		return err
	}
	return nil
}

func (c *Config) ToTomlFile(fPath string) error {
	fi, err := os.OpenFile(fPath, os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	_ = fi.Truncate(0)
	defer util.CloseFile(fi)

	if err := toml.NewEncoder(fi).Encode(c); err != nil {
		log.WithError(err).Error()
		return err
	}
	return nil
}

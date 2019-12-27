// Copyright 2019 StreamSets Inc.
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
package influxdb

import (
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"testing"
)

func getStageContext(
	configs []common.Config,
) *common.StageContextImpl {
	stageConfig := common.StageConfiguration{}
	stageConfig.Library = Library
	stageConfig.StageName = StageName
	stageConfig.Configuration = configs
	return &common.StageContextImpl{
		StageConfig: &stageConfig,
	}
}

func TestDestination_Init(t *testing.T) {
	configs := []common.Config{
		{
			Name:  "conf.url",
			Value: "http://localhost:8086",
		},
		{
			Name:  "conf.username",
			Value: "root",
		},
		{
			Name:  "conf.password",
			Value: "root",
		},
	}

	stageContext := getStageContext(configs)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}

	stageInstance := stageBean.Stage
	if stageInstance == nil {
		t.Error("Failed to create stage instance")
	}

	if stageInstance.(*Destination).Conf.Url != "http://localhost:8086" {
		t.Error("Failed to inject config value for url")
	}

	if stageInstance.(*Destination).Conf.Username != "root" {
		t.Error("Failed to inject config value for Username")
	}

	if stageInstance.(*Destination).Conf.Password != "root" {
		t.Error("Failed to inject config value for password")
	}
}

func TestStripPathPrefix(t *testing.T) {
	strippedValue := stripPathPrefix("/test1")
	if strippedValue != "test1" {
		t.Error("Failed to strip path prefix")
	}
}

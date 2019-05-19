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
package eventhubs

import (
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"testing"
)

func getStageContext(
	stageConfigurationList []common.Config,
	parameters map[string]interface{},
) *common.StageContextImpl {
	stageConfig := common.StageConfiguration{}
	stageConfig.Library = Library
	stageConfig.StageName = StageName
	stageConfig.Configuration = stageConfigurationList
	errorSink := common.NewErrorSink()
	return &common.StageContextImpl{
		StageConfig:       &stageConfig,
		Parameters:        parameters,
		ErrorSink:         errorSink,
		ErrorRecordPolicy: common.ErrorRecordPolicyStage,
	}
}

func TestDestination_Init(t *testing.T) {
	config := []common.Config{
		{
			Name:  "commonConf.sasKey",
			Value: "invalid-key",
		},
	}
	stageContext := getStageContext(config, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}

	stageInstance := stageBean.Stage
	if stageInstance == nil {
		t.Error("Failed to create stage instance")
	}

	if stageInstance.(*Destination).CommonConf.SasKey != "invalid-key" {
		t.Error("Failed to inject config value for SaS Key")
	}
}

func TestDestination_Init_Invalid_Connection_str(t *testing.T) {
	config := []common.Config{
		{
			Name:  "commonConf.sasKey",
			Value: "invalid-key",
		},
	}
	stageContext := getStageContext(config, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}

	stageInstance := stageBean.Stage
	if stageInstance == nil {
		t.Fatal("Failed to create stage instance")
	}

	if stageInstance.(*Destination).CommonConf.SasKey != "invalid-key" {
		t.Fatal("Failed to inject config value for SaS Key")
	}

	issues := stageInstance.Init(stageContext)

	if len(issues) != 1 {
		t.Fatal("Excepted error related to connection string")
	}

}

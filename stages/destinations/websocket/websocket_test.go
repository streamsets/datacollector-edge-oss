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
package websocket

import (
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"testing"
)

func getStageContext(
	resourceUrl string,
	headers []interface{},
	parameters map[string]interface{},
) *common.StageContextImpl {
	stageConfig := common.StageConfiguration{}
	stageConfig.Library = LIBRARY
	stageConfig.StageName = STAGE_NAME
	stageConfig.Configuration = []common.Config{
		{
			Name:  "conf.resourceUrl",
			Value: resourceUrl,
		},
		{
			Name:  "conf.headers",
			Value: headers,
		},
		{
			Name:  "conf.dataFormat",
			Value: "JSON",
		},
	}
	return &common.StageContextImpl{
		StageConfig: &stageConfig,
		Parameters:  parameters,
	}
}

func TestWebSocketClientDestination_Init(t *testing.T) {
	resourceUrl := "http://test:9000"
	headers := make([]interface{}, 2)
	headers[0] = map[string]interface{}{
		"key":   "X-SDC-APPLICATION-ID",
		"value": "SDC Edge",
	}
	headers[1] = map[string]interface{}{
		"key":   "DUMMY-HEADER",
		"value": "DUMMY",
	}

	stageContext := getStageContext(resourceUrl, headers, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}

	stageInstance := stageBean.Stage
	if stageInstance == nil {
		t.Error("Failed to create stage instance")
	}

	if stageInstance.(*WebSocketClientDestination).Conf.ResourceUrl != resourceUrl {
		t.Error("Failed to inject config value for resourceUrl")
	}

	if stageInstance.(*WebSocketClientDestination).Conf.Headers == nil {
		t.Error("Failed to inject config value for Headers")
		return
	}

	issues := stageInstance.Init(stageContext)
	if len(issues) != 0 {
		t.Error(issues[0].Message)
	}

	if stageInstance.(*WebSocketClientDestination).Conf.DataGeneratorFormatConfig.RecordWriterFactory == nil {
		t.Error("Failed to initialize RecordWriterFactory")
	}
}

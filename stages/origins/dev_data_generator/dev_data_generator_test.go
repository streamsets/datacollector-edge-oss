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
package dev_random

import (
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
	"testing"
)

func getStageContext(
	configuration []common.Config,
	parameters map[string]interface{},
) *common.StageContextImpl {
	stageConfig := common.StageConfiguration{}
	stageConfig.Library = Library
	stageConfig.StageName = StageName
	stageConfig.Configuration = configuration
	errorSink := common.NewErrorSink()
	return &common.StageContextImpl{
		StageConfig: &stageConfig,
		Parameters:  parameters,
		ErrorSink:   errorSink,
		EventSink:   common.NewEventSink(),
	}
}

func TestOrigin_Init(t *testing.T) {
	stageContext := getStageContext(getDefaultTestConfigs(), nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}

	stageInstance := stageBean.Stage
	if stageInstance == nil {
		t.Error("Failed to create stage instance")
	}

	if stageInstance.(*Origin).Delay != float64(1000) {
		t.Error("Failed to inject config value for delay")
	}

	if stageInstance.(*Origin).DataGenConfigs == nil {
		t.Error("Failed to inject config value for DataGenConfigs")
	}
}

func TestOrigin_Produce(t *testing.T) {
	stageContext := getStageContext(getDefaultTestConfigs(), nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}

	stageInstance := stageBean.Stage
	stageInstance.Init(stageContext)
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	_, err = stageInstance.(api.Origin).Produce(nil, 1, batchMaker)
	if err != nil {
		t.Error("Err :", err)
		return
	}
}

func getDefaultTestConfigs() []common.Config {
	dataGeneratorConfigList := []interface{}{
		map[string]interface{}{
			"field": "stringField",
			"type":  STRING,
		},
		map[string]interface{}{
			"field": "integerField",
			"type":  INTEGER,
		},
		map[string]interface{}{
			"field": "longField",
			"type":  LONG,
		},
		map[string]interface{}{
			"field": "floatField",
			"type":  FLOAT,
		},
		map[string]interface{}{
			"field": "doubleField",
			"type":  DOUBLE,
		},
		map[string]interface{}{
			"field": "boolField",
			"type":  BOOLEAN,
		},
		map[string]interface{}{
			"field": "dateTimeField",
			"type":  DATETIME,
		},
		map[string]interface{}{
			"field": "decimalField",
			"type":  DECIMAL,
		},
	}

	configuration := []common.Config{
		{
			Name:  "delay",
			Value: float64(1000),
		},
		{
			Name:  "dataGenConfigs",
			Value: dataGeneratorConfigList,
		},
	}

	return configuration
}

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
	"github.com/streamsets/datacollector-edge/api/fieldtype"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
	"strings"
	"testing"
)

func getStageContext(
	fields string,
	delay float64,
	maxRecordsToGenerate float64,
	parameters map[string]interface{},
) *common.StageContextImpl {
	stageConfig := common.StageConfiguration{}
	stageConfig.Library = LIBRARY
	stageConfig.StageName = STAGE_NAME
	stageConfig.Configuration = make([]common.Config, 3)
	stageConfig.Configuration[0] = common.Config{
		Name:  ConfFields,
		Value: fields,
	}
	stageConfig.Configuration[1] = common.Config{
		Name:  ConfDelay,
		Value: delay,
	}
	stageConfig.Configuration[2] = common.Config{
		Name:  ConfMaxRecordsToGenerate,
		Value: maxRecordsToGenerate,
	}
	return &common.StageContextImpl{
		StageConfig: &stageConfig,
		Parameters:  parameters,
	}
}

func TestDevRandom_Init(t *testing.T) {
	fields := "a,b,c"
	stageContext := getStageContext(fields, 10, 922337203685, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage

	if stageInstance.(*DevRandom).Fields != "a,b,c" {
		t.Error("Failed to inject config value for Fields")
	}

	if stageInstance.(*DevRandom).Delay != 10 {
		t.Error("Failed to inject config value for Delay")
	}
}

func TestDevRandomOrigin(t *testing.T) {
	fields := "a,b,c"
	stageContext := getStageContext(fields, 10, 922337203685, nil)

	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage

	issues := stageInstance.Init(stageContext)
	if len(issues) != 0 {
		t.Error(issues[0].Message)
	}

	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	_, err = stageInstance.(api.Origin).Produce(&randomOffset, 5, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	records := batchMaker.GetStageOutput()
	if len(records) != 5 {
		t.Error("Excepted 5 records but got - ", len(records))
	}

	for _, record := range records {
		rootField, _ := record.Get()
		if rootField.Type != fieldtype.MAP {
			t.Error("Excepted Map field type but got - ", rootField.Type, " Value: ", rootField.Value)
			return
		}

		rootFieldValue := rootField.Value.(map[string]*api.Field)
		for key, field := range rootFieldValue {
			if field.Type != fieldtype.LONG {
				t.Error("Excepted Long field type but got - ", field.Type, " Value: ", field.Value)
				return
			}
			if !strings.Contains(fields, key) {
				t.Error("Invalid key", key)
			}
		}
	}
	stageInstance.Destroy()
}

func TestDevRandom_Init_Parameter(t *testing.T) {
	fields := "${fields}"
	stageContext := getStageContext(fields, 10, 922337203685, nil)
	_, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err == nil || !strings.Contains(err.Error(), "No parameter 'fields' found") {
		t.Error("Excepted error - No parameter 'fields' found")
	}
}

func TestDevRandom_Init_StringEL(t *testing.T) {
	fields := "${str:trim(FIELDS_PARAM)}"
	parameters := map[string]interface{}{
		"FIELDS_PARAM": "x,y,z  ",
	}
	stageContext := getStageContext(fields, 10, 922337203685, parameters)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage
	issues := stageInstance.Init(stageContext)
	if len(issues) != 0 {
		t.Error(issues[0].Message)
	}

	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	_, err = stageInstance.(api.Origin).Produce(&randomOffset, 1, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	records := batchMaker.GetStageOutput()
	if len(records) != 1 {
		t.Error("Excepted 5 records but got - ", len(records))
	}

	for _, record := range records {
		rootField, _ := record.Get()
		if rootField.Type != fieldtype.MAP {
			t.Error("Excepted Map field type but got - ", rootField.Type, " Value: ", rootField.Value)
			return
		}

		rootFieldValue := rootField.Value.(map[string]*api.Field)
		for key, field := range rootFieldValue {
			if field.Type != fieldtype.LONG {
				t.Error("Excepted Long field type but got - ", field.Type, " Value: ", field.Value)
				return
			}
			if !((key == "x") || (key == "y") || (key == "z")) {
				t.Error("Invalid key", key)
			}
		}
	}
	stageInstance.Destroy()
}

func TestDevRandomOrigin_MaxRecordsToGenerate(t *testing.T) {
	fields := "a,b,c"
	stageContext := getStageContext(fields, 10, 3, nil)

	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage

	issues := stageInstance.Init(stageContext)
	if len(issues) != 0 {
		t.Error(issues[0].Message)
	}

	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	_, err = stageInstance.(api.Origin).Produce(&randomOffset, 5, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	records := batchMaker.GetStageOutput()
	if len(records) != 3 {
		t.Error("Excepted 3 records but got - ", len(records))
	}

	batchMaker = runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	_, err = stageInstance.(api.Origin).Produce(&randomOffset, 5, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	records = batchMaker.GetStageOutput()
	if len(records) != 0 {
		t.Error("Excepted 0 records but got - ", len(records))
	}
}

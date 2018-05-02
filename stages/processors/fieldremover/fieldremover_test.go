/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fieldremover

import (
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
	"strings"
	"testing"
)

func getStageContext(fields []interface{}, filterOperation string, parameters map[string]interface{}) *common.StageContextImpl {
	stageConfig := common.StageConfiguration{}
	stageConfig.Library = LIBRARY
	stageConfig.StageName = STAGE_NAME
	stageConfig.Configuration = make([]common.Config, 2)
	stageConfig.Configuration[0] = common.Config{
		Name:  FIELDS,
		Value: fields,
	}
	stageConfig.Configuration[1] = common.Config{
		Name:  FILTEROPERATION,
		Value: filterOperation,
	}
	return &common.StageContextImpl{
		StageConfig: &stageConfig,
		Parameters:  parameters,
	}
}

func TestFieldRemoverProcessor_Init(t *testing.T) {
	fields := []interface{}{"/a", "/b", "/c"}
	filterOperation := REMOVE
	stageContext := getStageContext(fields, filterOperation, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage

	if stageInstance.(*FieldRemoverProcessor).Fields == nil {
		t.Error("Failed to inject config value for Fields")
	}
}

func TestFieldRemoverProcessor_InitUnsupported(t *testing.T) {
	fields := []interface{}{"/a", "/b", "/c"}
	filterOperation := "SOMEFILTER"
	stageContext := getStageContext(fields, filterOperation, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage
	issues := stageInstance.Init(stageContext)
	if len(issues) == 0 || !strings.Contains(issues[0].Message, "Unsupported") {
		t.Error("Filter operation not properly flagged as unsupported")
	}
}

func TestFieldRemoverProcessor_InitUnexpected(t *testing.T) {
	fields := []interface{}{"/a", 11, "/c"}
	filterOperation := "KEEP"
	stageContext := getStageContext(fields, filterOperation, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage
	issues := stageInstance.Init(stageContext)
	if len(issues) == 0 || !strings.Contains(issues[0].Message, "Unexpected") {
		t.Error("Fields list integer not properly flagged as unexpected")
	}
}

func TestFieldRemoverProcessorRemove(t *testing.T) {
	fields := []interface{}{"/a", "/b", "/c", "/e/f"}
	filterOperation := REMOVE
	stageContext := getStageContext(fields, filterOperation, nil)

	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage

	issues := stageInstance.Init(stageContext)
	if len(issues) != 0 {
		t.Error(issues[0].Message)
	}

	records := make([]api.Record, 3)
	records[0], _ = stageContext.CreateRecord(
		"0",
		map[string]interface{}{"a": 123, "b": 456, "d": 78, "e": map[string]interface{}{"f": 1, "g": 2}},
	)
	records[1], _ = stageContext.CreateRecord("1", map[string]interface{}{"b": 456, "d": 78, "g": "9"})
	records[2], _ = stageContext.CreateRecord("2", map[string]interface{}{"x": nil, "y": 3e2, "z": 'a'})
	batch := runner.NewBatchImpl("fieldRemover", records, nil)
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{})

	err = stageInstance.(api.Processor).Process(batch, batchMaker)
	if err != nil {
		t.Error("Error in Identity Processor")
	}

	record := batchMaker.GetStageOutput()[0]
	if len(record.GetFieldPaths()) != 4 {
		t.Error("Fields not removed properly")
	}

	if f, err := record.Get("/e/g"); err != nil || f.Value != 2 {
		t.Error("Error reading nested field")
	}

	record = batchMaker.GetStageOutput()[1]
	if len(record.GetFieldPaths()) != 3 {
		t.Error("Fields not removed properly")
	}

	record = batchMaker.GetStageOutput()[2]
	if len(record.GetFieldPaths()) != 4 {
		t.Error("Fields not removed properly")
	}

	stageInstance.Destroy()
}

func TestFieldRemoverProcessorKeep(t *testing.T) {
	fields := []interface{}{"/a", "/b", "/c"}
	filterOperation := KEEP
	stageContext := getStageContext(fields, filterOperation, nil)

	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage

	issues := stageInstance.Init(stageContext)
	if len(issues) != 0 {
		t.Error(issues[0].Message)
	}

	records := make([]api.Record, 1)
	records[0], _ = stageContext.CreateRecord("1", map[string]interface{}{"a": 123, "b": 456, "d": 78})
	batch := runner.NewBatchImpl("fieldRemover", records, nil)
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{})

	err = stageInstance.(api.Processor).Process(batch, batchMaker)
	if err != nil {
		t.Error("Error in Identity Processor")
	}

	field, _ := batchMaker.GetStageOutput()[0].Get()
	if len(field.Value.(map[string]*api.Field)) != 2 {
		t.Error("Fields not removed properly")
	}

	stageInstance.Destroy()
}

func TestFieldRemoverProcessorRemoveNull(t *testing.T) {
	fields := []interface{}{"/a", "/b", "/c", "/e/h", "/e/i"}
	filterOperation := REMOVE_NULL
	stageContext := getStageContext(fields, filterOperation, nil)

	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage

	issues := stageInstance.Init(stageContext)
	if len(issues) != 0 {
		t.Error(issues[0].Message)
	}

	records := make([]api.Record, 1)
	records[0], _ = stageContext.CreateRecord(
		"1",
		map[string]interface{}{"a": 123, "b": 456, "d": 78, "c": nil, "g": nil,
			"e": map[string]interface{}{"h": nil, "i": 5}},
	)
	batch := runner.NewBatchImpl("fieldRemover", records, nil)
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{})

	err = stageInstance.(api.Processor).Process(batch, batchMaker)
	if err != nil {
		t.Error("Error in Identity Processor")
	}

	record := batchMaker.GetStageOutput()[0]
	if len(record.GetFieldPaths()) != 7 {
		t.Error("Fields not removed properly")
	}

	if f, err := record.Get("/e/i"); err != nil || f.Value != 5 {
		t.Error("Error reading nested field")
	}

	stageInstance.Destroy()
}

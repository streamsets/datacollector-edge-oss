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
package expression

import (
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
	"strings"
	"testing"
)

const (
	EXPRESSION_PROCESSOR_CONFIGS = "expressionProcessorConfigs"
	HEADER_ATTRIBUTE_CONFIGS     = "headerAttributeConfigs"
	FIELD_TO_SET                 = "fieldToSet"
	ATTRIBUTE_TO_SET             = "attributeToSet"
)

func getStageContext() (*common.StageContextImpl, *common.ErrorSink) {
	stageConfig := common.StageConfiguration{}
	stageConfig.Library = LIBRARY
	stageConfig.StageName = STAGE_NAME
	stageConfig.InstanceName = "expr1"
	stageConfig.Configuration = make([]common.Config, 2)

	fieldValueConfigs := []interface{}{}
	fieldValueConfigs = append(fieldValueConfigs, map[string]interface{}{
		FIELD_TO_SET: "/d",
		EXPRESSION:   "${math:ceil(record:value('/a'))}",
	})

	fieldValueConfigs = append(fieldValueConfigs, map[string]interface{}{
		FIELD_TO_SET: "/e",
		EXPRESSION:   "${math:floor(record:value('/b'))}",
	})

	headerAttributeConfigs := []interface{}{}
	headerAttributeConfigs = append(headerAttributeConfigs, map[string]interface{}{
		ATTRIBUTE_TO_SET: "eval",
		EXPRESSION:       "${str:toUpper(record:value('/c'))}",
	})

	stageConfig.Configuration[0] = common.Config{
		Name:  EXPRESSION_PROCESSOR_CONFIGS,
		Value: fieldValueConfigs,
	}

	stageConfig.Configuration[1] = common.Config{
		Name:  HEADER_ATTRIBUTE_CONFIGS,
		Value: headerAttributeConfigs,
	}

	errorSink := common.NewErrorSink()

	return &common.StageContextImpl{
		StageConfig:       &stageConfig,
		Parameters:        nil,
		ErrorSink:         errorSink,
		ErrorRecordPolicy: common.ErrorRecordPolicyStage,
	}, errorSink
}

func TestExpressionProcessor_Success(t *testing.T) {
	stageContext, errSink := getStageContext()
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters)
	if err != nil {
		t.Fatal(err)
	}

	stageInstance := stageBean.Stage.(*ExpressionProcessor)
	if stageInstance == nil {
		t.Fatal("Failed to create stage instance")
	}
	issues := stageInstance.Init(stageContext)
	if len(issues) != 0 {
		t.Error(issues[0].Message)
	}
	defer stageInstance.Destroy()

	records := make([]api.Record, 1)
	records[0], _ = stageContext.CreateRecord("abc", map[string]interface{}{"a": float64(2.55), "b": float64(3.55), "c": "random"})
	batch := runner.NewBatchImpl("random", records, "randomOffset")
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{})

	err = stageInstance.Process(batch, batchMaker)

	if err != nil {
		t.Fatal("Error when processing batch " + err.Error())
	}

	records = batchMaker.GetStageOutput()

	record := records[0]

	dValue, err := record.Get("/d")

	if err != nil {
		t.Error("Error when getting value of /d " + err.Error())
	}

	if dValue.Value.(float64) != float64(3) {
		t.Errorf("Error in expression processor when evaluating /d, Expected : 6. Actual:%d", dValue.Value)
	}

	eValue, err := record.Get("/e")

	if err != nil {
		t.Error("Error when getting value of /e " + err.Error())
	}

	if eValue.Value.(float64) != float64(3) {
		t.Errorf("Error in expression processor when evaluating /e, Expected : 5. Actual:%d", eValue.Value)
	}

	headers := record.GetHeader().GetAttributes()

	header, ok := headers["eval"]
	if !ok || strings.Compare(header, "RANDOM") != 0 {
		t.Errorf("Error in expression processor when evaluating header eval, Expected : random. Actual:%s", header)
	}

	if errSink.GetTotalErrorRecords() != 0 {
		t.Fatal("There should be no error records in error sink")
	}
}

func TestExpressionProcessor_Error(t *testing.T) {
	stageContext, errSink := getStageContext()

	stageContext.StageConfig.Configuration[1] = common.Config{
		Name: HEADER_ATTRIBUTE_CONFIGS,
		Value: []interface{}{map[string]interface{}{
			ATTRIBUTE_TO_SET: "eval",
			EXPRESSION:       "${unsupport:unsupported()}",
		}},
	}
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters)
	stageInstance := stageBean.Stage.(*ExpressionProcessor)
	if stageInstance == nil {
		t.Fatal("Failed to create stage instance")
	}
	issues := stageInstance.Init(stageContext)
	if len(issues) != 0 {
		t.Error(issues[0].Message)
	}
	defer stageInstance.Destroy()

	records := make([]api.Record, 1)
	records[0], _ = stageContext.CreateRecord("abc", map[string]interface{}{"a": float64(2.55), "b": float64(3.55), "c": "random"})
	batch := runner.NewBatchImpl("random", records, "randomOffset")
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{})
	err = stageInstance.Process(batch, batchMaker)

	if err != nil {
		t.Fatal("Error when processing batch " + err.Error())
	}

	if len(batchMaker.GetStageOutput()) != 0 {
		t.Fatal("The record should not be in batch maker and should have router to error")
	}

	if errSink.GetTotalErrorRecords() != 1 {
		t.Fatal("There should be one error record in error sink")
	}
}

func TestExpressionProcessor_DefaultConfig(t *testing.T) {
	stageContext, errSink := getStageContext()

	fieldValueConfigs := []interface{}{}
	fieldValueConfigs = append(fieldValueConfigs, map[string]interface{}{
		FIELD_TO_SET: "/",
		EXPRESSION:   "${record:value('/')}",
	})
	stageContext.StageConfig.Configuration[0].Value = fieldValueConfigs

	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters)
	if err != nil {
		t.Fatal(err)
	}

	stageInstance := stageBean.Stage.(*ExpressionProcessor)
	if stageInstance == nil {
		t.Fatal("Failed to create stage instance")
	}
	issues := stageInstance.Init(stageContext)
	if len(issues) != 0 {
		t.Error(issues[0].Message)
	}
	defer stageInstance.Destroy()

	records := make([]api.Record, 1)
	records[0], _ = stageContext.CreateRecord(
		"abc",
		map[string]interface{}{"a": float64(2.55), "b": float64(3.55), "c": "random"},
	)
	batch := runner.NewBatchImpl("random", records, "randomOffset")
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{})

	err = stageInstance.Process(batch, batchMaker)

	if err != nil {
		t.Fatal("Error when processing batch " + err.Error())
	}

	records = batchMaker.GetStageOutput()

	record := records[0]

	dValue, err := record.Get("/a")

	if err != nil {
		t.Error("Error when getting value of /a " + err.Error())
	}

	if dValue.Value.(float64) != float64(2.55) {
		t.Errorf("Error in expression processor when evaluating /d, Expected : 6. Actual:%d", dValue.Value)
	}

	if errSink.GetTotalErrorRecords() != 0 {
		t.Fatal("There should be no error records in error sink")
	}
}

// +build javascript

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
package javascript

import (
	"github.com/spf13/cast"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/fieldtype"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
	"testing"
)

const (
	ProcessingModeConfig = "processingMode"
	InitScriptConfig     = "initScript"
	ScriptConfig         = "script"
	DestroyScriptConfig  = "destroyScript"
)

func getStageContext(
	processingMode string,
	initScript string,
	script string,
	destroyScript string,
) (*common.StageContextImpl, *common.ErrorSink, *common.EventSink) {
	stageConfig := common.StageConfiguration{}
	stageConfig.Library = Library
	stageConfig.StageName = StageName
	stageConfig.InstanceName = "javascriptEvaluator"
	stageConfig.Configuration = []common.Config{
		{
			Name:  ProcessingModeConfig,
			Value: processingMode,
		},
		{
			Name:  InitScriptConfig,
			Value: initScript,
		},
		{
			Name:  ScriptConfig,
			Value: script,
		},
		{
			Name:  DestroyScriptConfig,
			Value: destroyScript,
		},
	}
	errorSink := common.NewErrorSink()
	eventSink := common.NewEventSink()
	return &common.StageContextImpl{
		StageConfig:       &stageConfig,
		Parameters:        map[string]interface{}{"param1": "param1Value"},
		ErrorSink:         errorSink,
		ErrorRecordPolicy: common.ErrorRecordPolicyStage,
		EventSink:         eventSink,
	}, errorSink, eventSink
}

func TestJavaScriptProcessor_Success(t *testing.T) {
	initScript := `state.counter = 1;`
	script := `
		for(var i = 0; i < records.length; i++) {
		  try {
            var record = records[i];
			record.value.a = 20.2 + state.counter;
            record.value.newMapField = { e: "eValue" };
            record.value.newArrayField = ['Element 1', 'Element 2'];
			output.write(record);
		  } catch (e) {
			// Send record to error
			error.Write(records[i], e);
		  }
		}
	`
	destroyScript := `state.counter = -1;`
	stageContext, errSink, _ := getStageContext(BatchProcessingMode, initScript, script, destroyScript)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Fatal(err)
	}

	stageInstance := stageBean.Stage.(*JavaScriptProcessor)
	if stageInstance == nil {
		t.Fatal("Failed to create stage instance")
	}
	issues := stageInstance.Init(stageContext)
	if len(issues) > 0 {
		t.Fatal("Error initializing stage context for the stage")
	}

	records := make([]api.Record, 1)
	records[0], _ = stageContext.CreateRecord(
		"abc",
		map[string]interface{}{
			"a": float64(2.55),
			"b": float64(3.55),
			"c": "random",
		},
	)
	batch := runner.NewBatchImpl("random", records, nil)
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)

	err = stageInstance.Process(batch, batchMaker)

	if err != nil {
		t.Fatal("Error when processing batch: " + err.Error())
	}

	records = batchMaker.GetStageOutput()

	if len(records) != 1 {
		t.Error("No output records generated")
		return
	}

	record := records[0]

	aValue, err := record.Get("/a")

	if err != nil {
		t.Error("Error when getting value of /a " + err.Error())
	}

	if aValue.Value.(float64) != float64(21.2) {
		t.Errorf("Error in javascript evaluator when evaluating /d, Expected : 20.2. Actual:%d", aValue.Value)
	}

	eValue, err := record.Get("/newMapField/e")

	if err != nil {
		t.Error("Error when getting value of /newMapField/e " + err.Error())
	}

	if eValue.Value.(string) != "eValue" {
		t.Errorf("Error in javascript evaluator when evaluating /newMapField/e, Expected : eValue. Actual:%s",
			aValue.Value)
	}

	if errSink.GetTotalErrorRecords() != 0 {
		t.Fatal("There should be no error records in error sink")
	}

	err = stageInstance.Destroy()

	if err != nil {
		t.Fatal("Error in destroy phase " + err.Error())
	}

	if stageInstance.state["counter"] != float64(-1) {
		t.Errorf("Error in javascript processor - destroy phase, Expected : -1 Actual:%d",
			stageInstance.state["counter"])
	}
}

func TestJavaScriptProcessor_Failure(t *testing.T) {
	initScript := `state.counter = 1;`
	script := `
		for(var i = 0; i < records.length; i++) {
		  try {
            var record = records[i];
			record.value.a = 20.2 + state.counter;
			output.Write(record.value.x.z);
		  } catch (e) {
			// Send record to error
			error.Write(records[i], e);
		  }
		}
	`
	destroyScript := `state.counter = -1;`
	stageContext, errSink, _ := getStageContext(BatchProcessingMode, initScript, script, destroyScript)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Fatal(err)
	}

	stageInstance := stageBean.Stage.(*JavaScriptProcessor)
	if stageInstance == nil {
		t.Fatal("Failed to create stage instance")
	}
	issues := stageInstance.Init(stageContext)
	if len(issues) > 0 {
		t.Fatal("Error initializing stage context for the stage")
	}

	records := make([]api.Record, 1)
	records[0], _ = stageContext.CreateRecord(
		"abc",
		map[string]interface{}{
			"a": float64(2.55),
			"b": float64(3.55),
			"c": "random",
		},
	)
	batch := runner.NewBatchImpl("random", records, nil)
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)

	err = stageInstance.Process(batch, batchMaker)

	if err != nil {
		t.Fatal("Error when processing batch " + err.Error())
	}

	records = batchMaker.GetStageOutput()

	if len(records) > 0 {
		t.Errorf("Error in javascript processor - Expected output: 0 Actual:%d", len(records))
	}

	if errSink.GetTotalErrorRecords() != 1 {
		t.Errorf("Error in javascript processor - Expected error records: 1 Actual:%d",
			errSink.GetTotalErrorRecords())
	}

	err = stageInstance.Destroy()

	if err != nil {
		t.Fatal("Error in destroy phase " + err.Error())
	}

	if stageInstance.state["counter"] != float64(-1) {
		t.Errorf("Error in javascript processor - destroy phase, Expected : -1 Actual:%d",
			stageInstance.state["counter"])
	}
}

func TestJavaScriptProcessor_TypedNullObject(t *testing.T) {
	initScript := `state.counter = 1;`
	script := `
		for(var i = 0; i < records.length; i++) {
		  try {
            var record = records[i];
			record.value.stringNull = NULL_STRING;
			record.value.floatNull = NULL_FLOAT;
			record.value.newField = 'newFieldValue';
			record.value.nullValue = 'hghgh';
			output.write(record);
		  } catch (e) {
			// Send record to error
			error.Write(records[i], e);
		  }
		}
	`
	destroyScript := `state.counter = -1;`
	stageContext, _, _ := getStageContext(BatchProcessingMode, initScript, script, destroyScript)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Fatal(err)
	}

	stageInstance := stageBean.Stage.(*JavaScriptProcessor)
	if stageInstance == nil {
		t.Fatal("Failed to create stage instance")
	}
	issues := stageInstance.Init(stageContext)
	if len(issues) > 0 {
		t.Fatal("Error initializing stage context for the stage")
	}

	records := make([]api.Record, 1)
	records[0], _ = stageContext.CreateRecord(
		"abc",
		map[string]interface{}{
			"a": float64(2.55),
			"b": float64(3.55),
			"c": "random",
		},
	)
	batch := runner.NewBatchImpl("random", records, nil)
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)

	err = stageInstance.Process(batch, batchMaker)

	if err != nil {
		t.Fatal("Error when processing batch: " + err.Error())
	}

	records = batchMaker.GetStageOutput()

	if len(records) != 1 {
		t.Error("No output records generated")
		return
	}

	record := records[0]

	stringNullValue, err := record.Get("/stringNull")
	if err != nil {
		t.Error("Error when getting value of /stringNull " + err.Error())
	}
	if stringNullValue.Value != nil {
		t.Errorf("Expected : nil. Actual:%d", stringNullValue.Value)
	}
	if stringNullValue.Type != fieldtype.STRING {
		t.Errorf("Expected : string. Actual:%s", stringNullValue.Type)
	}

	floatNullValue, err := record.Get("/floatNull")
	if err != nil {
		t.Error("Error when getting value of /floatNull " + err.Error())
	}
	if floatNullValue.Value != nil {
		t.Errorf("Expected : nil. Actual:%s", floatNullValue.Value)
	}
	if floatNullValue.Type != fieldtype.FLOAT {
		t.Errorf("Expected : float. Actual:%s", floatNullValue.Type)
	}

	newFieldValue, err := record.Get("/newField")
	if err != nil {
		t.Error("Error when getting value of /newField " + err.Error())
	}
	if newFieldValue.Value != "newFieldValue" {
		t.Errorf("Expected : newFieldValue. Actual:%s", newFieldValue.Value)
	}
	if newFieldValue.Type != fieldtype.STRING {
		t.Errorf("Expected : String. Actual:%s", newFieldValue.Type)
	}
}

func TestJavaScriptProcessor_SDCFunctions(t *testing.T) {
	initScript := `state.counter = 1;`
	script := `
		for(var i = 0; i < records.length; i++) {
		  try {
            var record = records[i];
			record.value.a = 20.2 + state.counter;
            record.value.newMapField = { e: "eValue" };
            record.value.newArrayField = ['Element 1', 'Element 2'];
			output.write(record);

			var newRecord = sdcFunctions.createRecord('newId');
			newRecord.value.a = 20.2 + state.counter;
			newRecord.value.b = sdcFunctions.createMap();
			newRecord.value.b.f1 = 'test';
			newRecord.value.previewMode = sdcFunctions.isPreview();
			newRecord.value.pipelineParameters = sdcFunctions.pipelineParameters();
			output.write(newRecord);

			var eventRecord = sdcFunctions.createEvent('sampleEventType', 1);
			sdcFunctions.toEvent(eventRecord);
		  } catch (e) {
			// Send record to error
			error.write(records[i], e);
		  }
		}
	`
	destroyScript := `state.counter = -1;`
	stageContext, errSink, eventSink := getStageContext(BatchProcessingMode, initScript, script, destroyScript)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Fatal(err)
	}

	stageInstance := stageBean.Stage.(*JavaScriptProcessor)
	if stageInstance == nil {
		t.Fatal("Failed to create stage instance")
	}
	issues := stageInstance.Init(stageContext)
	if len(issues) > 0 {
		t.Fatal("Error initializing stage context for the stage")
	}

	records := make([]api.Record, 1)
	records[0], _ = stageContext.CreateRecord(
		"abc",
		map[string]interface{}{
			"a": float64(2.55),
			"b": float64(3.55),
			"c": "random",
		},
	)
	batch := runner.NewBatchImpl("random", records, nil)
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)

	err = stageInstance.Process(batch, batchMaker)

	if err != nil {
		t.Fatal("Error when processing batch: " + err.Error())
	}

	if errSink.GetTotalErrorRecords() != 0 {
		errorRecord := errSink.GetStageErrorRecords("javascriptEvaluator")[0]
		t.Fatal("There should be no error records in error sink - " + errorRecord.GetHeader().GetErrorMessage())
	}

	records = batchMaker.GetStageOutput()

	if len(records) != 2 {
		t.Error("No output records generated")
		return
	}

	// Test sdcFunctions.createRecord
	record := records[1]
	aValue, err := record.Get("/a")
	if err != nil {
		t.Error("Error when getting value of /a " + err.Error())
	}

	if aValue.Value.(float64) != float64(21.2) {
		t.Errorf("Error in javascript evaluator when evaluating /d, Expected : 20.2. Actual:%d", aValue.Value)
	}

	// Test sdcFunctions.createMap()
	f1Value, err := record.Get("/b/f1")
	if err != nil {
		t.Error("Error when getting value of /b/f1 " + err.Error())
	}
	if cast.ToString(f1Value.Value) != "test" {
		t.Errorf("Error when evaluating /b/f1, Expected : 'test'. Actual:%d", f1Value.Value)
	}

	// Test sdcFunctions.isPreview()
	previewModeField, err := record.Get("/previewMode")
	if err != nil {
		t.Error("Error when getting value of /previewMode " + err.Error())
	}
	if cast.ToBool(previewModeField.Value) {
		t.Errorf("sdcFunctions.isPreview returned wrong value")
	}

	// Test sdcFunctions.pipelineParameters()
	param1Field, err := record.Get("/pipelineParameters/param1")
	if err != nil {
		t.Error("Error when getting value of /pipelineParameters/param1 " + err.Error())
	}
	if cast.ToString(param1Field.Value) != "param1Value" {
		t.Errorf("sdcFunctions.pipelineParameters returned wrong value")
	}

	// Test sdcFunctions.createEvent
	eventRecords := eventSink.GetStageEvents("javascriptEvaluator")
	if len(eventRecords) != 1 {
		t.Errorf("Failed to create event records")
	}

	if eventRecords[0].GetHeader().GetAttribute(api.EventRecordHeaderType) != "sampleEventType" {
		t.Errorf("Wrong event type")
	}

	if eventRecords[0].GetHeader().GetAttribute(api.EventRecordHeaderVersion) != "1" {
		t.Errorf("Wrong event version")
	}

	// Call destroy
	err = stageInstance.Destroy()
	if err != nil {
		t.Fatal("Error in destroy phase " + err.Error())
	}
}

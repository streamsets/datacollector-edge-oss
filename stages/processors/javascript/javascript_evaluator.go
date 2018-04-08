// +build javascript

/*
 * Copyright 2018 StreamSets Inc.
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

package javascript

import (
	"errors"
	"fmt"
	"github.com/robertkrimen/otto"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/lib/scripting"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"strings"
)

const (
	LIBRARY                = "streamsets-datacollector-basic-lib"
	STAGE_NAME             = "com_streamsets_pipeline_stage_processor_javascript_JavaScriptDProcessor"
	VERSION                = 2
	STATE                  = "state"
	RECORDS                = "records"
	OUTPUT                 = "output"
	ERROR                  = "error"
	RECORD_PROCESSING_MODE = "RECORD"
	BATCH_PROCESSING_MODE  = "BATCH"
)

type JavaScriptProcessor struct {
	*common.BaseStage
	ProcessingMode string `ConfigDef:"type=STRING,required=true"`
	InitScript     string `ConfigDef:"type=STRING,required=true"`
	Script         string `ConfigDef:"type=STRING,required=true"`
	DestroyScript  string `ConfigDef:"type=STRING,required=true"`
	state          map[string]interface{}
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &JavaScriptProcessor{BaseStage: &common.BaseStage{}}
	})
}

func (j *JavaScriptProcessor) Init(stageContext api.StageContext) []validation.Issue {
	issues := j.BaseStage.Init(stageContext)
	j.state = make(map[string]interface{})

	if j.InitScript != "" {
		vm := otto.New()
		vm.Set(STATE, j.state)
		_, err := vm.Run(j.InitScript)
		if err != nil {
			log.Error(fmt.Sprintf("Failed to execute init script code due to error: %s", err.Error()))
			issues = append(issues, stageContext.CreateConfigIssue(err.Error()))
			return issues
		}
	}

	j.Script = j.preProcessScript(j.Script)

	return issues
}

func (j *JavaScriptProcessor) Process(batch api.Batch, batchMaker api.BatchMaker) error {
	switch j.ProcessingMode {
	case RECORD_PROCESSING_MODE:
		return j.runRecordProcessingMode(batch, batchMaker)
	case BATCH_PROCESSING_MODE:
		return j.runBatchProcessingMode(batch, batchMaker)
	default:
		return errors.New("Invalid Processing mode")
	}
}

func (j *JavaScriptProcessor) runRecordProcessingMode(batch api.Batch, batchMaker api.BatchMaker) error {
	scriptObjectFactory := scripting.ScriptObjectFactory{Context: j.GetStageContext()}

	for _, record := range batch.GetRecords() {
		scriptRecords := make([]map[string]interface{}, 0)
		scriptRecord, err := scriptObjectFactory.CreateScriptRecord(record)
		if err != nil {
			return nil
		}
		scriptRecords = append(scriptRecords, scriptRecord)
		j.runScript(scriptRecords, j.GetStageContext(), batchMaker, scriptObjectFactory)
	}

	return nil
}

func (j *JavaScriptProcessor) runBatchProcessingMode(batch api.Batch, batchMaker api.BatchMaker) error {
	scriptObjectFactory := scripting.ScriptObjectFactory{Context: j.GetStageContext()}
	scriptRecords := make([]map[string]interface{}, 0)
	for _, record := range batch.GetRecords() {
		scriptRecord, err := scriptObjectFactory.CreateScriptRecord(record)
		if err != nil {
			return nil
		}
		scriptRecords = append(scriptRecords, scriptRecord)
	}
	return j.runScript(scriptRecords, j.GetStageContext(), batchMaker, scriptObjectFactory)
}

func (j *JavaScriptProcessor) runScript(
	scriptRecords []map[string]interface{},
	stageContext api.StageContext,
	batchMaker api.BatchMaker,
	scriptObjectFactory scripting.ScriptObjectFactory,
) error {
	vm := otto.New()
	vm.Set(RECORDS, scriptRecords)
	vm.Set(STATE, j.state)
	vm.Set(OUTPUT, &Out{
		batchMaker:          batchMaker,
		scriptObjectFactory: scriptObjectFactory,
		stageContext:        j.GetStageContext(),
	})
	vm.Set(ERROR, &Err{
		scriptObjectFactory: scriptObjectFactory,
		stageContext:        j.GetStageContext(),
	})
	vm.Set("NULL_BOOLEAN", scripting.NULL_BOOLEAN)
	vm.Set("NULL_CHAR", scripting.NULL_CHAR)
	vm.Set("NULL_BYTE", scripting.NULL_BYTE)
	vm.Set("NULL_SHORT", scripting.NULL_SHORT)
	vm.Set("NULL_INTEGER", scripting.NULL_INTEGER)
	vm.Set("NULL_LONG", scripting.NULL_LONG)
	vm.Set("NULL_FLOAT", scripting.NULL_FLOAT)
	vm.Set("NULL_DOUBLE", scripting.NULL_DOUBLE)
	vm.Set("NULL_DECIMAL", scripting.NULL_DECIMAL)
	vm.Set("NULL_BYTE_ARRAY", scripting.NULL_BYTE_ARRAY)
	vm.Set("NULL_STRING", scripting.NULL_STRING)
	vm.Set("NULL_LIST", scripting.NULL_LIST)
	vm.Set("NULL_MAP", scripting.NULL_MAP)

	_, err := vm.Run(j.Script)
	if err != nil {
		log.Error(fmt.Sprintf("Failed to execute JavaScript code due to error: %s", err.Error()))
		return err
	}

	return nil
}

func (j *JavaScriptProcessor) Destroy() error {
	if j.DestroyScript != "" {
		vm := otto.New()
		vm.Set(STATE, j.state)
		_, err := vm.Run(j.DestroyScript)
		if err != nil {
			log.Error(fmt.Sprintf("Failed to execute destroy script code due to error: %s", err.Error()))
			return err
		}
	}
	return j.BaseStage.Destroy()
}

func (j *JavaScriptProcessor) preProcessScript(script string) string {
	replacer := strings.NewReplacer(
		"output.write(", "output.Write(",
		"error.write(", "error.Write(",
	)
	return replacer.Replace(script)
}

type Out struct {
	stageContext        api.StageContext
	batchMaker          api.BatchMaker
	scriptObjectFactory scripting.ScriptObjectFactory
}

func (o *Out) Write(call otto.FunctionCall) otto.Value {
	val, err := call.Argument(0).Export()
	if err != nil {
		log.Error(fmt.Sprintf("Failed to read object from write: %s", err.Error()))
		o.stageContext.ReportError(err)
	}
	scriptRecord := val.(map[string]interface{})
	outputRecord, err := o.scriptObjectFactory.GetRecord(scriptRecord)
	if err != nil {
		log.Error(fmt.Sprintf("Failed to get record from script record: %s", err.Error()))
		o.stageContext.ReportError(err)
	}
	o.batchMaker.AddRecord(outputRecord)
	return otto.Value{}
}

type Err struct {
	stageContext        api.StageContext
	scriptObjectFactory scripting.ScriptObjectFactory
}

func (e *Err) Write(call otto.FunctionCall) otto.Value {
	val, err := call.Argument(0).Export()
	if err != nil {
		log.Error(fmt.Sprintf("Failed to read object from write: %s", err.Error()))
		e.stageContext.ReportError(err)
	}
	scriptRecord := val.(map[string]interface{})
	errorRecord, err := e.scriptObjectFactory.GetRecord(scriptRecord)
	if err != nil {
		log.Error(fmt.Sprintf("Failed to get record from script record: %s", err.Error()))
		e.stageContext.ReportError(err)
	}

	errorMessage := call.Argument(1).String()
	e.stageContext.ToError(errors.New(errorMessage), errorRecord)
	return otto.Value{}
}

type SdcFunctions struct {
}

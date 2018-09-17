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
	"errors"
	"fmt"
	"github.com/robertkrimen/otto"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cast"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/lib/scripting"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"strings"
)

const (
	Library              = "streamsets-datacollector-basic-lib"
	StageName            = "com_streamsets_pipeline_stage_processor_javascript_JavaScriptDProcessor"
	State                = "state"
	Records              = "records"
	Output               = "output"
	Error                = "error"
	SdcFunctions         = "sdcFunctions"
	RecordProcessingMode = "RECORD"
	BatchProcessingMode  = "BATCH"
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
	stagelibrary.SetCreator(Library, StageName, func() api.Stage {
		return &JavaScriptProcessor{BaseStage: &common.BaseStage{}}
	})
}

func (j *JavaScriptProcessor) Init(stageContext api.StageContext) []validation.Issue {
	issues := j.BaseStage.Init(stageContext)
	j.state = make(map[string]interface{})

	if j.InitScript != "" {
		vm := otto.New()
		vm.Set(State, j.state)
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
	case RecordProcessingMode:
		return j.runRecordProcessingMode(batch, batchMaker)
	case BatchProcessingMode:
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
			log.WithError(err).Error("Failed to create script record")
			j.GetStageContext().ToError(err, record)
			continue
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
			log.WithError(err).Error("Failed to create script record")
			j.GetStageContext().ToError(err, record)
			continue
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
	vm.Set(Records, scriptRecords)
	vm.Set(State, j.state)
	vm.Set(Output, &Out{
		batchMaker:          batchMaker,
		scriptObjectFactory: scriptObjectFactory,
		stageContext:        j.GetStageContext(),
	})
	vm.Set(Error, &Err{
		scriptObjectFactory: scriptObjectFactory,
		stageContext:        j.GetStageContext(),
	})
	vm.Set(SdcFunctions, &SDCEdgeFunctions{
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
		j.GetStageContext().ReportError(err)
	}

	return nil
}

func (j *JavaScriptProcessor) Destroy() error {
	if j.DestroyScript != "" {
		vm := otto.New()
		vm.Set(State, j.state)
		_, err := vm.Run(j.DestroyScript)
		if err != nil {
			log.Error(fmt.Sprintf("Failed to execute destroy script code due to error: %s", err.Error()))
			j.GetStageContext().ReportError(err)
		}
	}
	return j.BaseStage.Destroy()
}

func (j *JavaScriptProcessor) preProcessScript(script string) string {
	replacer := strings.NewReplacer(
		"output.write(", "output.Write(",
		"error.write(", "error.Write(",
		"sdcFunctions.getFieldNull(", "sdcFunctions.GetFieldNull(",
		"sdcFunctions.createRecord(", "sdcFunctions.CreateRecord(",
		"sdcFunctions.createMap(", "sdcFunctions.CreateMap(",
		"sdcFunctions.createEvent(", "sdcFunctions.CreateEvent(",
		"sdcFunctions.toEvent(", "sdcFunctions.ToEvent(",
		"sdcFunctions.isPreview(", "sdcFunctions.IsPreview(",
		"sdcFunctions.pipelineParameters(", "sdcFunctions.PipelineParameters(",
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
		return otto.Value{}
	}
	scriptRecord := val.(map[string]interface{})
	outputRecord, err := o.scriptObjectFactory.GetRecord(scriptRecord)
	if err != nil {
		log.Error(fmt.Sprintf("Failed to get record from script record: %s", err.Error()))
		o.stageContext.ReportError(err)
		return otto.Value{}
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
		return otto.Value{}
	}
	scriptRecord := val.(map[string]interface{})
	errorRecord, err := e.scriptObjectFactory.GetRecord(scriptRecord)
	if err != nil {
		log.Error(fmt.Sprintf("Failed to get record from script record: %s", err.Error()))
		e.stageContext.ReportError(err)
		return otto.Value{}
	}

	errorMessage := call.Argument(1).String()
	e.stageContext.ToError(errors.New(errorMessage), errorRecord)
	return otto.Value{}
}

type SDCEdgeFunctions struct {
	stageContext        api.StageContext
	scriptObjectFactory scripting.ScriptObjectFactory
}

func (s *SDCEdgeFunctions) GetFieldNull(call otto.FunctionCall) otto.Value {
	val, err := call.Argument(0).Export()
	if err != nil {
		log.Error(fmt.Sprintf("Failed to read object from write: %s", err.Error()))
		s.stageContext.ReportError(err)
		return otto.Value{}
	}
	scriptRecord := val.(map[string]interface{})
	record, err := s.scriptObjectFactory.GetRecord(scriptRecord)
	if err != nil {
		log.Error(fmt.Sprintf("Failed to get record from script record: %s", err.Error()))
		s.stageContext.ReportError(err)
		return otto.Value{}
	}

	secondArg, err := call.Argument(1).Export()
	if err != nil {
		log.Error(fmt.Sprintf("Failed to read field path from getFieldNull: %s", err.Error()))
		s.stageContext.ReportError(err)
		return otto.Value{}
	}
	fieldPath := cast.ToString(secondArg)

	nullValue, err := scripting.GetFieldNull(record, fieldPath)
	if err != nil {
		log.Error(fmt.Sprintf("Failed to getFieldNull: %s", err.Error()))
		s.stageContext.ReportError(err)
		return otto.Value{}
	}

	vm := otto.New()
	ottoValue, err := vm.ToValue(nullValue)
	if err != nil {
		log.WithError(err).Error("CreateMap: Failed to get otto value")
		return otto.Value{}
	}

	return ottoValue
}

func (s *SDCEdgeFunctions) CreateRecord(call otto.FunctionCall) otto.Value {
	val, err := call.Argument(0).Export()
	if err != nil {
		log.WithError(err).Error("Failed to read object from createRecord")
		s.stageContext.ReportError(err)
		return otto.Value{}
	}
	recordId := cast.ToString(val)
	newRecord, err := s.stageContext.CreateRecord(recordId, make(map[string]interface{}))
	if err != nil {
		log.WithError(err).Error("Failed to create new record")
		s.stageContext.ReportError(err)
		return otto.Value{}
	}

	scriptRecord, err := s.scriptObjectFactory.CreateScriptRecord(newRecord)
	if err != nil {
		log.WithError(err).Error("Failed to create script record")
		s.stageContext.ReportError(err)
		return otto.Value{}
	}

	vm := otto.New()
	value, err := vm.ToValue(scriptRecord)
	if err != nil {
		log.WithError(err).Error("Failed to get otto value")
		return otto.FalseValue()
	}
	return value
}

func (s *SDCEdgeFunctions) CreateEvent(call otto.FunctionCall) otto.Value {
	arg1Val, err := call.Argument(0).Export()
	if err != nil {
		log.WithError(err).Error("Failed to read first argument from CreateEvent")
		s.stageContext.ReportError(err)
		return otto.Value{}
	}
	eventType := cast.ToString(arg1Val)

	arg2Val, err := call.Argument(1).Export()
	if err != nil {
		log.WithError(err).Error("Failed to read second argument from CreateEvent")
		s.stageContext.ReportError(err)
		return otto.Value{}
	}
	eventVersion := cast.ToInt(arg2Val)

	newEventRecord, err := s.stageContext.CreateEventRecord(
		"evenId",
		make(map[string]interface{}),
		eventType,
		eventVersion,
	)
	if err != nil {
		log.WithError(err).Error("Failed to create new record")
		s.stageContext.ReportError(err)
		return otto.Value{}
	}

	scriptRecord, err := s.scriptObjectFactory.CreateScriptRecord(newEventRecord)
	if err != nil {
		log.WithError(err).Error("Failed to create script record")
		s.stageContext.ReportError(err)
		return otto.Value{}
	}

	vm := otto.New()
	value, err := vm.ToValue(scriptRecord)
	if err != nil {
		log.WithError(err).Error("Failed to get otto value")
		return otto.FalseValue()
	}
	return value
}

func (s *SDCEdgeFunctions) ToEvent(call otto.FunctionCall) otto.Value {
	val, err := call.Argument(0).Export()
	if err != nil {
		log.WithError(err).Error("Failed to read object from toEvent")
		s.stageContext.ReportError(err)
		return otto.Value{}
	}
	scriptRecord := val.(map[string]interface{})
	eventRecord, err := s.scriptObjectFactory.GetRecord(scriptRecord)
	if err != nil {
		log.WithError(err).Error("Failed to get record from script record")
		s.stageContext.ReportError(err)
		return otto.Value{}
	}
	s.stageContext.ToEvent(eventRecord)
	return otto.Value{}
}

func (s *SDCEdgeFunctions) IsPreview(call otto.FunctionCall) otto.Value {
	value, err := otto.ToValue(s.stageContext.IsPreview())
	if err != nil {
		log.WithError(err).Error("IsPreview: Failed to get otto value")
		return otto.FalseValue()
	}
	return value
}

func (s *SDCEdgeFunctions) CreateMap(call otto.FunctionCall) otto.Value {
	vm := otto.New()
	mapValue, err := vm.ToValue(make(map[string]interface{}))
	if err != nil {
		log.WithError(err).Error("CreateMap: Failed to get otto value")
		return otto.Value{}
	}
	return mapValue
}

func (s *SDCEdgeFunctions) PipelineParameters(call otto.FunctionCall) otto.Value {
	vm := otto.New()
	mapValue, err := vm.ToValue(s.stageContext.GetPipelineParameters())
	if err != nil {
		log.WithError(err).Error("PipelineParameters: Failed to get otto value")
		return otto.Value{}
	}
	return mapValue
}

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
package common

import (
	"context"
	"errors"
	"fmt"
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/el"
	"github.com/streamsets/datacollector-edge/container/util"
	"strconv"
	"strings"
	"time"
)

const StageConfig = "STAGE_CONFIG"

type StageContextImpl struct {
	StageConfig       *StageConfiguration
	Parameters        map[string]interface{}
	Metrics           metrics.Registry
	ErrorSink         *ErrorSink
	EventSink         *EventSink
	ErrorStage        bool
	ErrorRecordPolicy string
	Services          map[string]api.Service
	ElContext         context.Context
	previewMode       bool
	stop              bool
}

func (s *StageContextImpl) GetResolvedValue(configValue interface{}) (interface{}, error) {
	var err error
	switch t := configValue.(type) {
	case string:
		return s.resolveIfImplicitEL(configValue.(string))
	case []interface{}:
		for i, val := range t {
			t[i], err = s.GetResolvedValue(val)
			if err != nil {
				return nil, err
			}
		}
		return configValue, nil
	case map[string]interface{}:
		for k, v := range t {
			t[k], err = s.GetResolvedValue(v)
			if err != nil {
				return nil, err
			}
		}
		return configValue, nil
	default:
		return configValue, nil
	}
}

func (s *StageContextImpl) resolveIfImplicitEL(configValue string) (interface{}, error) {
	if el.IsElString(configValue) {
		return el.Evaluate(configValue, "configName", s.Parameters, nil)
	} else {
		return configValue, nil
	}
}

func (s *StageContextImpl) GetParameterValue(paramName string) interface{} {
	paramName = strings.Replace(paramName, el.PARAMETER_PREFIX, "", 1)
	paramName = strings.Replace(paramName, el.PARAMETER_SUFFIX, "", 1)

	if p, err := strconv.ParseInt(s.Parameters[paramName].(string), 10, 64); err == nil {
		return p
	}

	return s.Parameters[paramName]
}

func (s *StageContextImpl) GetMetrics() metrics.Registry {
	return s.Metrics
}

func (s *StageContextImpl) CreateRecord(recordSourceId string, value interface{}) (api.Record, error) {
	record, err := createRecord(recordSourceId, value)
	if err != nil {
		return nil, err
	}
	headerImplForRecord := record.GetHeader().(*HeaderImpl)
	headerImplForRecord.SetStageCreator(s.StageConfig.InstanceName)
	if s.ErrorRecordPolicy == ErrorRecordPolicyOriginal {
		// Clone the current record to the header for error record handling
		headerImplForRecord.SetSourceRecord(record.Clone())
	}
	return record, err
}

func (s *StageContextImpl) CreateEventRecord(
	recordSourceId string,
	value interface{},
	eventType string,
	eventVersion int,
) (api.Record, error) {
	record, err := createRecord(recordSourceId, value)
	if err != nil {
		return nil, err
	}
	headerImplForRecord := record.GetHeader().(*HeaderImpl)
	headerImplForRecord.SetStageCreator(s.StageConfig.InstanceName)
	if s.ErrorRecordPolicy == ErrorRecordPolicyOriginal {
		// Clone the current record to the header for error record handling
		headerImplForRecord.SetSourceRecord(record.Clone())
	}
	AddStageToStagePath(headerImplForRecord, s.StageConfig.InstanceName)
	CreateTrackingId(headerImplForRecord)
	headerImplForRecord.SetAttribute(api.EventRecordHeaderType, eventType)
	headerImplForRecord.SetAttribute(api.EventRecordHeaderVersion, strconv.Itoa(eventVersion))
	headerImplForRecord.SetAttribute(
		api.EventRecordHeaderCreationTimestamp,
		strconv.FormatInt(util.ConvertTimeToLong(time.Now()), 10),
	)
	return record, err
}

func (s *StageContextImpl) ToError(err error, record api.Record) {
	errorRecord := constructErrorRecord(s.StageConfig.InstanceName, err, s.ErrorRecordPolicy, record)
	s.ErrorSink.ToError(s.StageConfig.InstanceName, errorRecord)
}

func (s *StageContextImpl) ToEvent(record api.Record) {
	s.EventSink.AddEvent(s.StageConfig.InstanceName, record)
}

func (s *StageContextImpl) ReportError(err error) {
	errorMessage := constructErrorMessage(err)
	s.ErrorSink.ReportError(s.StageConfig.InstanceName, errorMessage)
}

func (s *StageContextImpl) GetOutputLanes() []string {
	return s.StageConfig.OutputLanes
}

func (s *StageContextImpl) Evaluate(
	value string,
	configName string,
	ctx context.Context,
) (interface{}, error) {
	if el.IsElString(value) {
		evaluator, _ := el.NewEvaluator(
			configName,
			s.Parameters,
			[]el.Definitions{
				&el.StringEL{},
				&el.MathEL{},
				&el.RecordEL{Context: ctx},
				&el.MapListEL{},
				&el.PipelineEL{Context: s.ElContext},
				&el.JobEL{Context: s.ElContext},
				&el.SdcEL{},
			},
		)
		return evaluator.Evaluate(value)
	} else {
		return value, nil
	}
}

func (s *StageContextImpl) IsErrorStage() bool {
	return s.ErrorStage
}

// optional argument, first optional argument is configGroup, second optional argument- configName
func (s *StageContextImpl) CreateConfigIssue(error string, optional ...interface{}) validation.Issue {
	issue := validation.Issue{
		InstanceName: s.StageConfig.InstanceName,
		Level:        StageConfig,
		Count:        1,
		Message:      error,
	}

	if len(optional) > 0 {
		issue.ConfigGroup = optional[0].(string)
	}

	if len(optional) > 1 {
		issue.ConfigName = optional[1].(string)
	}

	return issue
}

func (s *StageContextImpl) GetService(serviceName string) (api.Service, error) {
	if s.Services[serviceName] != nil {
		return s.Services[serviceName], nil
	}
	return nil, errors.New(fmt.Sprintf("No Service instance found for service name: %s", serviceName))
}

func (s *StageContextImpl) IsPreview() bool {
	return s.previewMode
}

func (s *StageContextImpl) GetPipelineParameters() map[string]interface{} {
	return s.Parameters
}

func (s *StageContextImpl) SetStop() {
	s.stop = true
}

func (s *StageContextImpl) IsStopped() bool {
	return s.stop
}

func constructErrorRecord(instanceName string, err error, errorRecordPolicy string, record api.Record) api.Record {
	var recordToBeSentToError api.Record
	headerForRecord := record.GetHeader().(*HeaderImpl)
	switch errorRecordPolicy {
	case ErrorRecordPolicyStage:
		recordToBeSentToError = record
	case ErrorRecordPolicyOriginal:
		recordToBeSentToError = headerForRecord.GetSourceRecord()
	default:
		log.Errorf("Unsupported Error Record Policy: %s, Using the original record from source", errorRecordPolicy)
		recordToBeSentToError = headerForRecord.GetSourceRecord()
	}
	headerImplForRecord := recordToBeSentToError.GetHeader().(*HeaderImpl)
	headerImplForRecord.SetErrorStageInstance(instanceName)
	headerImplForRecord.SetErrorMessage(err.Error())
	headerImplForRecord.SetErrorTimeStamp(util.ConvertTimeToLong(time.Now()))

	if len(headerImplForRecord.StagesPath) == 0 {
		headerImplForRecord.SetStagesPath(headerForRecord.StagesPath)
		headerImplForRecord.SetTrackingId(headerForRecord.TrackingId)
	}

	return recordToBeSentToError
}

func constructErrorMessage(err error) api.ErrorMessage {
	errorMessage := api.ErrorMessage{}
	errorMessage.LocalizableMessage = err.Error()
	errorMessage.Timestamp = util.ConvertTimeToLong(time.Now())
	return errorMessage
}

func CreateRecordId(prefix string, counter int) string {
	return fmt.Sprintf("%s:%d", prefix, counter)
}

func NewStageContext(
	stageConfig *StageConfiguration,
	resolvedParameters map[string]interface{},
	metricRegistry metrics.Registry,
	errorSink *ErrorSink,
	errorStage bool,
	errorRecordPolicy string,
	services map[string]api.Service,
	elContext context.Context,
	eventSink *EventSink,
	isPreview bool,
) (*StageContextImpl, error) {
	stageContext := &StageContextImpl{
		StageConfig:       stageConfig,
		Parameters:        resolvedParameters,
		Metrics:           metricRegistry,
		ErrorSink:         errorSink,
		EventSink:         eventSink,
		ErrorStage:        errorStage,
		ErrorRecordPolicy: errorRecordPolicy,
		Services:          services,
		ElContext:         elContext,
		previewMode:       isPreview,
	}

	return stageContext, nil
}

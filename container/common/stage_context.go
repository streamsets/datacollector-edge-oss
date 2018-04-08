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
package common

import (
	"context"
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
	ErrorStage        bool
	ErrorRecordPolicy string
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
		return el.Evaluate(configValue, "configName", s.Parameters)
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

func (s *StageContextImpl) ToError(err error, record api.Record) {
	errorRecord := constructErrorRecord(s.StageConfig.InstanceName, err, s.ErrorRecordPolicy, record)
	s.ErrorSink.ToError(s.StageConfig.InstanceName, errorRecord)
}

func (s *StageContextImpl) ReportError(err error) {
	s.ErrorSink.ReportError(s.StageConfig.InstanceName, err)
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

func constructErrorRecord(instanceName string, err error, errorRecordPolicy string, record api.Record) api.Record {
	var recordToBeSentToError api.Record
	switch errorRecordPolicy {
	case ErrorRecordPolicyStage:
		recordToBeSentToError = record
	case ErrorRecordPolicyOriginal:
		headerForRecord := record.GetHeader().(*HeaderImpl)
		recordToBeSentToError = headerForRecord.GetSourceRecord()
	default:
		log.Errorf("Unsupported Error Record Policy: %s, Using the original record from source", errorRecordPolicy)
		headerForRecord := record.GetHeader().(*HeaderImpl)
		recordToBeSentToError = headerForRecord.GetSourceRecord()
	}
	headerImplForRecord := recordToBeSentToError.GetHeader().(*HeaderImpl)
	headerImplForRecord.SetErrorStageInstance(instanceName)
	headerImplForRecord.SetErrorMessage(err.Error())
	headerImplForRecord.SetErrorTimeStamp(util.ConvertTimeToLong(time.Now()))
	return recordToBeSentToError
}

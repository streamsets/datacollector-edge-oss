package common

import (
	"context"
	"github.com/rcrowley/go-metrics"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/el"
	"github.com/streamsets/datacollector-edge/container/util"
	"strconv"
	"strings"
	"time"
)

type StageContextImpl struct {
	StageConfig StageConfiguration
	Parameters  map[string]interface{}
	Metrics     metrics.Registry
	ErrorSink   *ErrorSink
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
	return record, err
}

func (s *StageContextImpl) ToError(err error, record api.Record) {
	s.ErrorSink.ToError(
		s.StageConfig.InstanceName,
		constructErrorRecord(s.StageConfig.InstanceName, err, record),
	)
}

func (s *StageContextImpl) ReportError(err error) {
	s.ErrorSink.ReportError(
		s.StageConfig.InstanceName,
		err,
	)
}

func (s *StageContextImpl) GetOutputLanes() []string {
	return s.StageConfig.OutputLanes
}

func (s *StageContextImpl) Evaluate(
	value string,
	configName string,
	ctx context.Context,
) (interface{}, error) {
	evaluator, _ := el.NewEvaluator(
		configName,
		s.Parameters,
		[]el.Definitions{&el.StringEL{}, &el.RecordEL{Context: ctx}},
	)
	return evaluator.Evaluate(value)
}

func constructErrorRecord(instanceName string, err error, record api.Record) api.Record {
	// TODO: revisit this if we support processors
	// no need to clone the record, look for original record to be added to error lane
	// as the record is not transformed anywhere (i.e no processors in between at the moment)
	headerImplForRecord := record.GetHeader().(*HeaderImpl)
	headerImplForRecord.SetErrorStageInstance(instanceName)
	headerImplForRecord.SetErrorMessage(err.Error())
	headerImplForRecord.SetErrorTimeStamp(util.ConvertTimeToLong(time.Now()))
	return record
}

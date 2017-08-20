package common

import (
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

func (s *StageContextImpl) GetResolvedValue(configValue interface{}) interface{} {
	switch t := configValue.(type) {
	case string:
		if s.IsParameter(configValue.(string)) {
			return s.GetParameterValue(configValue.(string))
		} else {
			return configValue
		}
		break
	case []interface{}:
		for i, val := range t {
			t[i] = s.GetResolvedValue(val)
		}
		return configValue
	case map[string]interface{}:
		for k, v := range t {
			t[k] = s.GetResolvedValue(v)
		}
		return configValue
	default:
		return configValue
	}
	return configValue
}

func (s *StageContextImpl) IsParameter(configValue string) bool {
	return strings.HasPrefix(configValue, el.PARAMETER_PREFIX) &&
		strings.HasSuffix(configValue, el.PARAMETER_SUFFIX)
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

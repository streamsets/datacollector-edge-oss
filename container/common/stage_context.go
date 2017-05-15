package common

import (
	"strings"
	"strconv"
)

type StageContext struct {
	StageConfig StageConfiguration
	Parameters  map[string]interface{}
}

const (
	PARAMETER_PREFIX = "${"
	PARAMETER_SUFFIX = "}"
)

func (s *StageContext) GetResolvedValue(configValue interface{}) interface{} {
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

func (s *StageContext) IsParameter(configValue string) bool {
	return strings.HasPrefix(configValue, PARAMETER_PREFIX) &&
		strings.HasSuffix(configValue, PARAMETER_SUFFIX)
}

func (s *StageContext) GetParameterValue(paramName string) interface{} {
	paramName = strings.Replace(paramName, PARAMETER_PREFIX, "", 1)
	paramName = strings.Replace(paramName, PARAMETER_SUFFIX, "", 1)

	if p, err := strconv.ParseInt(s.Parameters[paramName].(string),10,64); err == nil {
		return p
	}

	return s.Parameters[paramName]
}

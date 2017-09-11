package el

import "strings"

func IsElString(configValue string) bool {
	return strings.HasPrefix(configValue, PARAMETER_PREFIX) &&
		strings.HasSuffix(configValue, PARAMETER_SUFFIX)
}

func Evaluate(value string,  configName string, parameters map[string]interface{}) (interface{}, error) {
	evaluator, _ := NewEvaluator(
		configName,
		parameters,
		[]Definitions{
			&StringEL{},
		},
	)
	return evaluator.Evaluate(value)
}


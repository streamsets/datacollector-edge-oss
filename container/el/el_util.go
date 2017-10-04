package el

import "strings"

const (
	NAMESPACE_FN_SEPARATOR = ":"
)

func IsElString(configValue string) bool {
	return strings.HasPrefix(configValue, PARAMETER_PREFIX) &&
		strings.HasSuffix(configValue, PARAMETER_SUFFIX)
}

func Evaluate(value string, configName string, parameters map[string]interface{}) (interface{}, error) {
	evaluator, _ := NewEvaluator(
		configName,
		parameters,
		[]Definitions{
			&StringEL{},
		},
	)
	return evaluator.Evaluate(value)
}

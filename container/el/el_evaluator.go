package el

import (
	"github.com/madhukard/govaluate"
	"strings"
)

const (
	PARAMETER_PREFIX = "${"
	PARAMETER_SUFFIX = "}"
)

type ELEvaluator struct {
	configName string
	parameters map[string]interface{}
	functions  map[string]govaluate.ExpressionFunction
}

type ELDefinitions interface {
	GetELFunctionDefinitions() map[string]govaluate.ExpressionFunction
}

func (elEvaluator *ELEvaluator) Evaluate(expression string) (interface{}, error) {
	if len(expression) == 0 {
		return expression, nil
	}
	expression = strings.Replace(expression, PARAMETER_PREFIX, "", 1)
	if strings.HasSuffix(expression, PARAMETER_SUFFIX) {
		expression = expression[:len(expression)-1]
	}

	evaluableExpression, err := govaluate.NewEvaluableExpressionWithFunctions(expression, elEvaluator.functions)
	if err != nil {
		return nil, err
	}
	result, err := evaluableExpression.Evaluate(elEvaluator.parameters)
	return result, err
}

func NewELEvaluator(
	configName string,
	parameters map[string]interface{},
	definitionsList []ELDefinitions,
) (*ELEvaluator, error) {
	var elEvaluator *ELEvaluator
	functions := make(map[string]govaluate.ExpressionFunction)

	if len(definitionsList) > 0 {
		for _, definitions := range definitionsList {
			for k, v := range definitions.GetELFunctionDefinitions() {
				functions[k] = v
			}
		}
	}

	elEvaluator = &ELEvaluator{
		configName: configName,
		parameters: parameters,
		functions:  functions,
	}
	return elEvaluator, nil
}

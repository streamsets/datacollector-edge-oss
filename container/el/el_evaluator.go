package el

import (
	"github.com/madhukard/govaluate"
	"strings"
)

const (
	PARAMETER_PREFIX = "${"
	PARAMETER_SUFFIX = "}"
)

type Evaluator struct {
	configName string
	parameters map[string]interface{}
	functions  map[string]govaluate.ExpressionFunction
}

type Definitions interface {
	GetELFunctionDefinitions() map[string]govaluate.ExpressionFunction
}

func (elEvaluator *Evaluator) Evaluate(expression string) (interface{}, error) {
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

	if err != nil {
		return nil, err
	}

	// TODO: Convert type based on config definition type
	/*
	if p, err := strconv.ParseFloat(result.(string), 64); err == nil {
		return p, nil
	}
	*/

	return result, err
}

func NewEvaluator(
	configName string,
	parameters map[string]interface{},
	definitionsList []Definitions,
) (*Evaluator, error) {
	var evaluator *Evaluator
	functions := make(map[string]govaluate.ExpressionFunction)

	if len(definitionsList) > 0 {
		for _, definitions := range definitionsList {
			for k, v := range definitions.GetELFunctionDefinitions() {
				functions[k] = v
			}
		}
	}

	evaluator = &Evaluator{
		configName: configName,
		parameters: parameters,
		functions:  functions,
	}
	return evaluator, nil
}

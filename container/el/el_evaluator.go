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

	if parameters == nil {
		parameters = make(map[string]interface{})
	}
	parameters["NULL"] = nil

	evaluator = &Evaluator{
		configName: configName,
		parameters: parameters,
		functions:  functions,
	}
	return evaluator, nil
}

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
	"errors"
	"fmt"
	"github.com/madhukard/govaluate"
	"math"
	"reflect"
)

const (
	WRONG_ARGS_MESSAGE          = "Wrong number of arguments '%d' to function '%s', Expected : '%d'"
	CAST_TO_FLOAT_ERROR_MESSAGE = "Cannot convert argument idx: '%d' with value '%v' and type '%v' to float64 for operation '%s'"
	MATH_PREFIX                 = "math"
	ABS                         = "abs"
	CEIL                        = "ceil"
	FLOOR                       = "floor"
	MAX                         = "max"
	MIN                         = "min"
)

type MathEL struct {
}

func (m *MathEL) checkArgsAndConvertToFloat64(funcName string, numberOfArgs int, args ...interface{}) ([]float64, error) {
	result := []float64{}
	if len(args) != numberOfArgs {
		return result, errors.New(
			fmt.Sprintf(WRONG_ARGS_MESSAGE, len(args), MATH_PREFIX+NAMESPACE_FN_SEPARATOR+funcName, 1),
		)
	}
	for idx, arg := range args {
		f, ok := arg.(float64)
		if !ok {
			return result, errors.New(
				fmt.Sprintf(CAST_TO_FLOAT_ERROR_MESSAGE, idx, arg, reflect.TypeOf(arg), funcName),
			)
		}
		result = append(result, f)
	}

	return result, nil
}

func (m *MathEL) Abs(args ...interface{}) (interface{}, error) {
	result, err := m.checkArgsAndConvertToFloat64(ABS, 1, args...)
	if err != nil {
		return nil, err
	}
	return math.Abs(result[0]), nil
}

func (m *MathEL) Ceil(args ...interface{}) (interface{}, error) {
	result, err := m.checkArgsAndConvertToFloat64(CEIL, 1, args...)
	if err != nil {
		return nil, err
	}
	return math.Ceil(result[0]), nil
}

func (m *MathEL) Floor(args ...interface{}) (interface{}, error) {
	result, err := m.checkArgsAndConvertToFloat64(FLOOR, 1, args...)
	if err != nil {
		return nil, err
	}
	return math.Floor(result[0]), nil
}

func (m *MathEL) Max(args ...interface{}) (interface{}, error) {
	result, err := m.checkArgsAndConvertToFloat64(MAX, 2, args...)
	if err != nil {
		return nil, err
	}
	return math.Max(result[0], result[1]), nil
}

func (m *MathEL) Min(args ...interface{}) (interface{}, error) {
	result, err := m.checkArgsAndConvertToFloat64(MIN, 2, args...)
	if err != nil {
		return nil, err
	}
	return math.Min(result[0], result[1]), nil
}

func (m *MathEL) GetELFunctionDefinitions() map[string]govaluate.ExpressionFunction {
	return map[string]govaluate.ExpressionFunction{
		MATH_PREFIX + NAMESPACE_FN_SEPARATOR + ABS:   m.Abs,
		MATH_PREFIX + NAMESPACE_FN_SEPARATOR + CEIL:  m.Ceil,
		MATH_PREFIX + NAMESPACE_FN_SEPARATOR + FLOOR: m.Floor,
		MATH_PREFIX + NAMESPACE_FN_SEPARATOR + MAX:   m.Max,
		MATH_PREFIX + NAMESPACE_FN_SEPARATOR + MIN:   m.Min,
		//"math:round" : m.Round, //TODO:SDCE-98
	}
}

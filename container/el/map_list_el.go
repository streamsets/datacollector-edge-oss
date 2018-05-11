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
	"reflect"
)

type MapListEL struct {
}

func (mapListEL *MapListEL) EmptyMap(args ...interface{}) (interface{}, error) {
	return make(map[string]interface{}), nil
}

func (mapListEL *MapListEL) Size(args ...interface{}) (interface{}, error) {
	if len(args) < 1 || args[0] == nil {
		return "", errors.New(
			fmt.Sprintf("The function 'size' requires 1 arguments but was passed %d", len(args)),
		)
	}
	return getMapLength(args[0], "size()")
}

func (mapListEL *MapListEL) IsEmptyMap(args ...interface{}) (interface{}, error) {
	if len(args) < 1 || args[0] == nil {
		return "", errors.New(
			fmt.Sprintf("The function 'isEmptyMap' requires 1 arguments but was passed %d", len(args)),
		)
	}

	mapLength, err := getMapLength(args[0], "isEmptyMap()")
	if err != nil {
		return nil, err
	}
	return mapLength == 0, nil
}

func (mapListEL *MapListEL) EmptyList(args ...interface{}) (interface{}, error) {
	return make([]interface{}, 0), nil
}

func (mapListEL *MapListEL) Length(args ...interface{}) (interface{}, error) {
	if len(args) < 1 || args[0] == nil {
		return "", errors.New(
			fmt.Sprintf("The function 'length' requires 1 arguments but was passed %d", len(args)),
		)
	}
	return getListLength(args[0], "length()")
}

func (mapListEL *MapListEL) IsEmptyList(args ...interface{}) (interface{}, error) {
	if len(args) < 1 || args[0] == nil {
		return "", errors.New(
			fmt.Sprintf("The function 'isEmptyList' requires 1 arguments but was passed %d", len(args)),
		)
	}

	listLength, err := getListLength(args[0], "isEmptyList()")
	if err != nil {
		return nil, err
	}
	return listLength == 0, nil
}

func getMapLength(arg interface{}, elFunctionName string) (interface{}, error) {
	switch t := arg.(type) {
	case map[string]interface{}:
		mapObject := arg.(map[string]interface{})
		return len(mapObject), nil
	case map[interface{}]interface{}:
		mapObject := arg.(map[string]interface{})
		return len(mapObject), nil
	default:
		return nil, errors.New(fmt.Sprintf(
			"Unsupported Field Type: %s for EL function %s",
			reflect.TypeOf(t),
			elFunctionName,
		))
	}
}

func getListLength(arg interface{}, elFunctionName string) (interface{}, error) {
	switch t := arg.(type) {
	case []interface{}:
		listObject := arg.([]interface{})
		return len(listObject), nil
	case []string:
		listObject := arg.([]string)
		return len(listObject), nil
	default:
		return nil, errors.New(fmt.Sprintf(
			"Unsupported Field Type: %s for EL function %s",
			reflect.TypeOf(t),
			elFunctionName,
		))
	}
}

func (mapListEL *MapListEL) GetELFunctionDefinitions() map[string]govaluate.ExpressionFunction {
	functions := map[string]govaluate.ExpressionFunction{
		"emptyMap":    mapListEL.EmptyMap,
		"size":        mapListEL.Size,
		"isEmptyMap":  mapListEL.IsEmptyMap,
		"emptyList":   mapListEL.EmptyList,
		"length":      mapListEL.Length,
		"isEmptyList": mapListEL.IsEmptyList,
	}
	return functions
}

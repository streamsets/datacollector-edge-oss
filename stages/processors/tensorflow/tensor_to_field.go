// +build tensorflow

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

package tensorflow

import (
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/fieldtype"
	tf "github.com/tensorflow/tensorflow/tensorflow/go"
)

func ConvertTensorToField(tensor *tf.Tensor) (*api.Field, error) {
	switch tensor.DataType() {
	case tf.Float:
		return CreateFloatField(tensor)
	case tf.Double:
		return CreateDoubleField(tensor)
	case tf.Int32:
		return CreateInt32Field(tensor)
	case tf.Uint8:
		return CreateUint8Field(tensor)
	case tf.String:
		return CreateStringField(tensor)
	case tf.Int64:
		return CreateInt64Field(tensor)
	case tf.Bool:
		return CreateBoolField(tensor)
	}
	return api.CreateField("Not implemented yet")
}

func CreateFloatField(tensor *tf.Tensor) (*api.Field, error) {
	floatVal := tensor.Value().([][]float32)
	rootValue := make([]*api.Field, 0)

	for _, firstVal := range floatVal {
		listFieldValue := make([]*api.Field, 0)
		for _, value := range firstVal {
			valField, err := api.CreateField(value)
			if err != nil {
				return nil, err
			}
			listFieldValue = append(listFieldValue, valField)
		}
		listField := &api.Field{Type: fieldtype.LIST, Value: listFieldValue}
		rootValue = append(rootValue, listField)
	}

	rootField := &api.Field{Type: fieldtype.LIST, Value: rootValue}

	return rootField, nil
}

func CreateDoubleField(tensor *tf.Tensor) (*api.Field, error) {
	doubleVal := tensor.Value().([][]float64)
	rootValue := make([]*api.Field, 0)

	for _, firstVal := range doubleVal {
		listFieldValue := make([]*api.Field, 0)
		for _, value := range firstVal {
			valField, err := api.CreateField(value)
			if err != nil {
				return nil, err
			}
			listFieldValue = append(listFieldValue, valField)
		}
		listField := &api.Field{Type: fieldtype.LIST, Value: listFieldValue}
		rootValue = append(rootValue, listField)
	}

	rootField := &api.Field{Type: fieldtype.LIST, Value: rootValue}

	return rootField, nil
}

func CreateInt32Field(tensor *tf.Tensor) (*api.Field, error) {
	doubleVal := tensor.Value().([][]int32)
	rootValue := make([]*api.Field, 0)

	for _, firstVal := range doubleVal {
		listFieldValue := make([]*api.Field, 0)
		for _, value := range firstVal {
			valField, err := api.CreateField(value)
			if err != nil {
				return nil, err
			}
			listFieldValue = append(listFieldValue, valField)
		}
		listField := &api.Field{Type: fieldtype.LIST, Value: listFieldValue}
		rootValue = append(rootValue, listField)
	}

	rootField := &api.Field{Type: fieldtype.LIST, Value: rootValue}

	return rootField, nil
}

func CreateUint8Field(tensor *tf.Tensor) (*api.Field, error) {
	doubleVal := tensor.Value().([][]uint8)
	rootValue := make([]*api.Field, 0)

	for _, firstVal := range doubleVal {
		listFieldValue := make([]*api.Field, 0)
		for _, value := range firstVal {
			valField, err := api.CreateField(value)
			if err != nil {
				return nil, err
			}
			listFieldValue = append(listFieldValue, valField)
		}
		listField := &api.Field{Type: fieldtype.LIST, Value: listFieldValue}
		rootValue = append(rootValue, listField)
	}

	rootField := &api.Field{Type: fieldtype.LIST, Value: rootValue}

	return rootField, nil
}

func CreateInt64Field(tensor *tf.Tensor) (*api.Field, error) {
	intVal := tensor.Value().([][]int64)
	rootValue := make([]*api.Field, 0)

	for _, firstVal := range intVal {
		listFieldValue := make([]*api.Field, 0)
		for _, value := range firstVal {
			valField, err := api.CreateField(value)
			if err != nil {
				return nil, err
			}
			listFieldValue = append(listFieldValue, valField)
		}
		listField := &api.Field{Type: fieldtype.LIST, Value: listFieldValue}
		rootValue = append(rootValue, listField)
	}

	rootField := &api.Field{Type: fieldtype.LIST, Value: rootValue}

	return rootField, nil
}

func CreateStringField(tensor *tf.Tensor) (*api.Field, error) {
	intVal := tensor.Value().([][]string)
	rootValue := make([]*api.Field, 0)

	for _, firstVal := range intVal {
		listFieldValue := make([]*api.Field, 0)
		for _, value := range firstVal {
			valField, err := api.CreateField(value)
			if err != nil {
				return nil, err
			}
			listFieldValue = append(listFieldValue, valField)
		}
		listField := &api.Field{Type: fieldtype.LIST, Value: listFieldValue}
		rootValue = append(rootValue, listField)
	}

	rootField := &api.Field{Type: fieldtype.LIST, Value: rootValue}

	return rootField, nil
}

func CreateBoolField(tensor *tf.Tensor) (*api.Field, error) {
	intVal := tensor.Value().([][]bool)
	rootValue := make([]*api.Field, 0)

	for _, firstVal := range intVal {
		listFieldValue := make([]*api.Field, 0)
		for _, value := range firstVal {
			valField, err := api.CreateField(value)
			if err != nil {
				return nil, err
			}
			listFieldValue = append(listFieldValue, valField)
		}
		listField := &api.Field{Type: fieldtype.LIST, Value: listFieldValue}
		rootValue = append(rootValue, listField)
	}

	rootField := &api.Field{Type: fieldtype.LIST, Value: rootValue}

	return rootField, nil
}

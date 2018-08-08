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
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	tf "github.com/tensorflow/tensorflow/tensorflow/go"
)

func ConvertFieldToTensor(record api.Record, inputConfig TensorInputConfig, op *tf.Operation) (*tf.Tensor, error) {
	switch inputConfig.TensorDataType {
	case "FLOAT":
		return CreateFloatTensor(record, inputConfig, op)
	case "DOUBLE":
		return CreateDoubleTensor(record, inputConfig, op)
	case "INT32":
		return CreateInt32Tensor(record, inputConfig, op)
	case "UINT8":
		return CreateUint8Tensor(record, inputConfig, op)
	case "STRING":
		return CreateStringTensor(record, inputConfig, op)
	case "INT64":
		return CreateInt64Tensor(record, inputConfig, op)
	case "BOOL":
		return CreateBoolTensor(record, inputConfig, op)
	}

	return nil, errors.New(fmt.Sprintf(
		"ConvertFieldToTensorfield type '%s' not supported",
		inputConfig.TensorDataType,
	))
}

func CreateFloatTensor(record api.Record, inputConfig TensorInputConfig, op *tf.Operation) (*tf.Tensor, error) {
	var err error
	fieldValues := make([]float32, len(inputConfig.Fields))
	for i, fieldPath := range inputConfig.Fields {
		var field *api.Field
		field, err = record.Get(fieldPath)
		if err != nil {
			log.WithError(err).Error("Failed to get field values")
			break
		}
		if field == nil || field.Value == nil {
			errorMsg := fmt.Sprintf("value for field path: %s is null", fieldPath)
			err = errors.New(errorMsg)
			log.Error(errorMsg)
			break
		}

		floatVal, err := field.GetValueAsFloat()
		if err != nil {
			return nil, err
		}
		fieldValues[i] = float32(floatVal)
	}

	if op.NumInputs() == 1 {
		// 2D Tensor
		return tf.NewTensor([][]float32{fieldValues})
	} else {
		return tf.NewTensor(fieldValues)
	}
}

func CreateDoubleTensor(record api.Record, inputConfig TensorInputConfig, op *tf.Operation) (*tf.Tensor, error) {
	var err error
	fieldValues := make([]float64, len(inputConfig.Fields))
	for i, fieldPath := range inputConfig.Fields {
		var field *api.Field
		field, err = record.Get(fieldPath)
		if err != nil {
			log.WithError(err).Error("Failed to get field values")
			break
		}
		if field == nil || field.Value == nil {
			errorMsg := fmt.Sprintf("value for field path: %s is null", fieldPath)
			err = errors.New(errorMsg)
			log.Error(errorMsg)
			break
		}
		fieldValues[i] = field.Value.(float64)
	}
	if op.NumInputs() == 1 {
		// 2D Tensor
		return tf.NewTensor([][]float64{fieldValues})
	} else {
		return tf.NewTensor(fieldValues)
	}
}

func CreateInt32Tensor(record api.Record, inputConfig TensorInputConfig, op *tf.Operation) (*tf.Tensor, error) {
	var err error
	fieldValues := make([]int32, len(inputConfig.Fields))
	for i, fieldPath := range inputConfig.Fields {
		var field *api.Field
		field, err = record.Get(fieldPath)
		if err != nil {
			log.WithError(err).Error("Failed to get field values")
			break
		}
		if field == nil || field.Value == nil {
			errorMsg := fmt.Sprintf("value for field path: %s is null", fieldPath)
			err = errors.New(errorMsg)
			log.Error(errorMsg)
			break
		}
		fieldValues[i] = field.Value.(int32)
	}
	if op.NumInputs() == 1 {
		// 2D Tensor
		return tf.NewTensor([][]int32{fieldValues})
	} else {
		return tf.NewTensor(fieldValues)
	}
}

func CreateUint8Tensor(record api.Record, inputConfig TensorInputConfig, op *tf.Operation) (*tf.Tensor, error) {
	var err error
	fieldValues := make([]uint8, len(inputConfig.Fields))
	for i, fieldPath := range inputConfig.Fields {
		var field *api.Field
		field, err = record.Get(fieldPath)
		if err != nil {
			log.WithError(err).Error("Failed to get field values")
			break
		}
		if field == nil || field.Value == nil {
			errorMsg := fmt.Sprintf("value for field path: %s is null", fieldPath)
			err = errors.New(errorMsg)
			log.Error(errorMsg)
			break
		}
		fieldValues[i] = field.Value.(uint8)
	}
	if op.NumInputs() == 1 {
		// 2D Tensor
		return tf.NewTensor([][]uint8{fieldValues})
	} else {
		return tf.NewTensor(fieldValues)
	}
}

func CreateStringTensor(record api.Record, inputConfig TensorInputConfig, op *tf.Operation) (*tf.Tensor, error) {
	var err error
	fieldValues := make([]string, len(inputConfig.Fields))
	for i, fieldPath := range inputConfig.Fields {
		var field *api.Field
		field, err = record.Get(fieldPath)
		if err != nil {
			log.WithError(err).Error("Failed to get field values")
			break
		}
		if field == nil || field.Value == nil {
			errorMsg := fmt.Sprintf("value for field path: %s is null", fieldPath)
			err = errors.New(errorMsg)
			log.Error(errorMsg)
			break
		}
		fieldValues[i] = field.Value.(string)
	}
	if op.NumInputs() == 1 {
		// 2D Tensor
		return tf.NewTensor([][]string{fieldValues})
	} else {
		return tf.NewTensor(fieldValues)
	}
}

func CreateInt64Tensor(record api.Record, inputConfig TensorInputConfig, op *tf.Operation) (*tf.Tensor, error) {
	var err error
	fieldValues := make([]int64, len(inputConfig.Fields))
	for i, fieldPath := range inputConfig.Fields {
		var field *api.Field
		field, err = record.Get(fieldPath)
		if err != nil {
			log.WithError(err).Error("Failed to get field values")
			break
		}
		if field == nil || field.Value == nil {
			errorMsg := fmt.Sprintf("value for field path: %s is null", fieldPath)
			err = errors.New(errorMsg)
			log.Error(errorMsg)
			break
		}
		fieldValues[i] = field.Value.(int64)
	}
	if op.NumInputs() == 1 {
		// 2D Tensor
		return tf.NewTensor([][]int64{fieldValues})
	} else {
		return tf.NewTensor(fieldValues)
	}
}

func CreateBoolTensor(record api.Record, inputConfig TensorInputConfig, op *tf.Operation) (*tf.Tensor, error) {
	var err error
	fieldValues := make([]bool, len(inputConfig.Fields))
	for i, fieldPath := range inputConfig.Fields {
		var field *api.Field
		field, err = record.Get(fieldPath)
		if err != nil {
			log.WithError(err).Error("Failed to get field values")
			break
		}
		if field == nil || field.Value == nil {
			errorMsg := fmt.Sprintf("value for field path: %s is null", fieldPath)
			err = errors.New(errorMsg)
			log.Error(errorMsg)
			break
		}
		fieldValues[i] = field.Value.(bool)
	}
	if op.NumInputs() == 1 {
		// 2D Tensor
		return tf.NewTensor([][]bool{fieldValues})
	} else {
		return tf.NewTensor(fieldValues)
	}
}

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
package api

import (
	"errors"
	"fmt"
	"github.com/streamsets/datacollector-edge/api/fieldtype"
	"github.com/streamsets/datacollector-edge/api/linkedhashmap"
	"math/big"
	"reflect"
	"strconv"
	"time"
)

type Field struct {
	Type  string
	Value interface{}
}

func (f *Field) Clone() *Field {
	switch f.Type {
	case fieldtype.MAP:
		mapField := f.Value.(map[string](*Field))
		returnMap := map[string](*Field){}
		for k, v := range mapField {
			returnMap[k] = v.Clone()
		}
		return &Field{Type: f.Type, Value: returnMap}
	case fieldtype.LIST_MAP:
		mapField := f.Value.(*linkedhashmap.Map)
		returnListMap := linkedhashmap.New()
		it := mapField.Iterator()
		for it.HasNext() {
			entry := it.Next()
			key := entry.GetKey()
			value := entry.GetValue().(*Field)
			returnListMap.Put(key, value.Clone())
		}
		return &Field{Type: f.Type, Value: returnListMap}
	case fieldtype.LIST:
		listField := f.Value.([](*Field))
		returnList := make([](*Field), len(listField))
		for idx, v := range listField {
			returnList[idx] = v.Clone()
		}
		return &Field{Type: f.Type, Value: returnList}
	default:
		return &Field{Type: f.Type, Value: f.Value}
	}
}

func (f *Field) GetValueAsFloat() (float32, error) {
	switch f.Type {
	case fieldtype.FLOAT:
		return f.Value.(float32), nil
	case fieldtype.STRING:
		strVal := f.Value.(string)
		float64Val, err := strconv.ParseFloat(strVal, 32)
		if err != nil {
			return 0, err
		}
		return float32(float64Val), nil
	case fieldtype.DOUBLE:
		return float32(f.Value.(float64)), nil
	case fieldtype.INTEGER:
		return float32(f.Value.(int32)), nil
	case fieldtype.LONG:
		return float32(f.Value.(int64)), nil
	case fieldtype.SHORT:
		return float32(f.Value.(int8)), nil
	}
	return 0, errors.New("cannot convert field value to float")
}

func CreateField(value interface{}) (*Field, error) {
	if value == nil {
		return CreateStringField("")
	}
	var err error = nil
	switch value.(type) {
	case bool:
		return CreateBoolField(value.(bool))
	case []byte:
		return CreateByteArrayField(value.([]byte))
	case byte:
		return CreateByteField(value.(byte))
	case int8:
		return CreateShortField(value.(int8))
	case int32:
		return CreateInteger32Field(value.(int32))
	case int:
		return CreateIntegerField(value.(int))
	case int64:
		return CreateLongField(value.(int64))
	case uint16:
		return CreateUInteger16Field(value.(uint16))
	case uint32:
		return CreateUInteger32Field(value.(uint32))
	case uint64:
		return CreateLongFieldU64(value.(uint64))
	case float32:
		return CreateFloatField(value.(float32))
	case float64:
		return CreateDoubleField(value.(float64))
	case big.Int:
		return CreateBigIntField(value.(big.Int))
	case big.Float:
		return CreateBigFloatField(value.(big.Float))
	case string:
		return CreateStringField(value.(string))
	case []string:
		return CreateStringListField(value.([]string))
	case []float64:
		return CreateFloatListField(value.([]float64))
	case []map[string]interface{}:
		return CreateMapListField(value.([]map[string]interface{}))
	case []interface{}:
		return CreateListField(value.([]interface{}))
	case map[string]interface{}:
		return CreateMapField(value.(map[string]interface{}))
	case *linkedhashmap.Map:
		return CreateListMapField(value.(*linkedhashmap.Map))
	case time.Time:
		return CreateDateTimeField(value.(time.Time))
	case FileRef:
		return CreateFileRefField(value.(FileRef))
	default:
		err = errors.New(fmt.Sprintf("Unsupported Field Type %s", reflect.TypeOf(value)))
	}
	return nil, err
}

func CreateBoolField(value bool) (*Field, error) {
	return &Field{Type: fieldtype.BOOLEAN, Value: value}, nil
}

func CreateByteArrayField(value []byte) (*Field, error) {
	return &Field{Type: fieldtype.BYTE_ARRAY, Value: value}, nil
}

func CreateByteField(value byte) (*Field, error) {
	return &Field{Type: fieldtype.BYTE, Value: value}, nil
}

func CreateDateTimeField(value time.Time) (*Field, error) {
	return &Field{Type: fieldtype.DATETIME, Value: value}, nil
}

func CreateShortField(value int8) (*Field, error) {
	return &Field{Type: fieldtype.SHORT, Value: value}, nil
}

func CreateIntegerField(value int) (*Field, error) {
	return &Field{Type: fieldtype.INTEGER, Value: value}, nil
}

func CreateInteger32Field(value int32) (*Field, error) {
	return &Field{Type: fieldtype.INTEGER, Value: value}, nil
}

func CreateUInteger16Field(value uint16) (*Field, error) {
	return &Field{Type: fieldtype.INTEGER, Value: value}, nil
}

func CreateUInteger32Field(value uint32) (*Field, error) {
	return &Field{Type: fieldtype.INTEGER, Value: value}, nil
}

func CreateLongFieldU64(value uint64) (*Field, error) {
	return &Field{Type: fieldtype.LONG, Value: value}, nil
}

func CreateLongField(value int64) (*Field, error) {
	return &Field{Type: fieldtype.LONG, Value: value}, nil
}

func CreateFloatField(value float32) (*Field, error) {
	return &Field{Type: fieldtype.FLOAT, Value: value}, nil
}

func CreateDoubleField(value float64) (*Field, error) {
	return &Field{Type: fieldtype.DOUBLE, Value: value}, nil
}

func CreateBigIntField(value big.Int) (*Field, error) {
	return &Field{Type: fieldtype.DECIMAL, Value: value}, nil
}

func CreateBigFloatField(value big.Float) (*Field, error) {
	return &Field{Type: fieldtype.DECIMAL, Value: value}, nil
}

func CreateStringField(value string) (*Field, error) {
	return &Field{Type: fieldtype.STRING, Value: value}, nil
}

func CreateFileRefField(value FileRef) (*Field, error) {
	return &Field{Type: fieldtype.FILE_REF, Value: value}, nil
}

func CreateStringListField(listStringValue []string) (*Field, error) {
	listFieldValue := make([]*Field, len(listStringValue))
	for i, value := range listStringValue {
		valField, err := CreateField(value)
		if err != nil {
			return nil, err
		}
		listFieldValue[i] = valField
	}
	listField := Field{Type: fieldtype.LIST, Value: listFieldValue}
	return &listField, nil
}

func CreateFloatListField(listFloatValue []float64) (*Field, error) {
	listFieldValue := make([]*Field, len(listFloatValue))
	for i, value := range listFloatValue {
		valField, err := CreateField(value)
		if err != nil {
			return nil, err
		}
		listFieldValue[i] = valField
	}
	listField := Field{Type: fieldtype.LIST, Value: listFieldValue}
	return &listField, nil
}

func CreateMapField(mapValue map[string]interface{}) (*Field, error) {
	mapFieldValue := make(map[string]*Field)
	for key, value := range mapValue {
		valField, err := CreateField(value)
		if err != nil {
			return nil, err
		}
		mapFieldValue[key] = valField
	}
	mapField := Field{Type: fieldtype.MAP, Value: mapFieldValue}
	return &mapField, nil
}

func CreateListMapField(listMapValue *linkedhashmap.Map) (*Field, error) {
	listMapFieldValue := linkedhashmap.New()
	it := listMapValue.Iterator()
	for it.HasNext() {
		entry := it.Next()
		key := entry.GetKey()
		value := entry.GetValue()
		valField, err := CreateField(value)
		if err != nil {
			return nil, err
		}
		listMapFieldValue.Put(key, valField)
	}
	mapField := Field{Type: fieldtype.LIST_MAP, Value: listMapFieldValue}
	return &mapField, nil
}

func CreateMapListField(listValue []map[string]interface{}) (*Field, error) {
	listFieldValue := make([]*Field, 0)
	for _, value := range listValue {
		valField, err := CreateField(value)
		if err != nil {
			return nil, err
		}
		listFieldValue = append(listFieldValue, valField)
	}
	listField := Field{Type: fieldtype.LIST, Value: listFieldValue}
	return &listField, nil
}

func CreateListField(listValue []interface{}) (*Field, error) {
	listFieldValue := make([]*Field, 0)
	for _, value := range listValue {
		valField, err := CreateField(value)
		if err != nil {
			return nil, err
		}
		listFieldValue = append(listFieldValue, valField)
	}
	listField := Field{Type: fieldtype.LIST, Value: listFieldValue}
	return &listField, nil
}

func CreateListFieldWithListOfFields(listFields []*Field) *Field {
	return &Field{Type: fieldtype.LIST, Value: listFields}
}

func CreateMapFieldWithMapOfFields(mapFields map[string]*Field) *Field {
	return &Field{Type: fieldtype.MAP, Value: mapFields}
}

func CreateListMapFieldWithMapOfFields(mapFields *linkedhashmap.Map) *Field {
	return &Field{Type: fieldtype.LIST_MAP, Value: mapFields}
}

func Create(fieldType string, value interface{}) (*Field, error) {
	return &Field{Type: fieldType, Value: value}, nil
}

func CreateFieldFromSDCField(value interface{}) (*Field, error) {
	switch value.(type) {
	case []*Field:
		return CreateListFieldWithListOfFields(value.([]*Field)), nil
	case map[string]*Field:
		return CreateMapFieldWithMapOfFields(value.(map[string]*Field)), nil
	case *linkedhashmap.Map:
		return CreateListMapFieldWithMapOfFields(value.(*linkedhashmap.Map)), nil
	case *Field:
		f := value.(*Field)
		return &Field{Type: f.Type, Value: f.Value}, nil
	default:
		return CreateField(value)
	}
}

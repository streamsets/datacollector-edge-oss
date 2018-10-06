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

package scripting

import (
	"github.com/robertkrimen/otto"
	"github.com/spf13/cast"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/fieldtype"
	"github.com/streamsets/datacollector-edge/api/linkedhashmap"
	"math/big"
	"strconv"
	"time"
)

type ScriptObjectFactory struct {
	Context api.StageContext
}

func (s *ScriptObjectFactory) CreateScriptRecord(record api.Record) (map[string]interface{}, error) {
	var scriptValue interface{}
	recordValue, err := record.Get()
	if err != nil {
		return nil, err
	}
	if recordValue != nil {
		scriptValue, err = s.fieldToScript(recordValue)
		if err != nil {
			return nil, err
		}
	}
	return NewScriptRecord(record, scriptValue)
}

func (s *ScriptObjectFactory) GetRecord(scriptRecord map[string]interface{}) (api.Record, error) {
	record := scriptRecord["record"].(api.Record)
	field, err := s.scriptToField(scriptRecord["value"], record, "")
	if err != nil {
		return nil, err
	}
	record.Set(field)
	// Update Record Header Attributes
	s.updateRecordHeader(scriptRecord["attributes"].(map[string]string), record)
	return record, nil
}

func (s *ScriptObjectFactory) fieldToScript(field *api.Field) (interface{}, error) {
	var scriptObject interface{}
	if field != nil {
		scriptObject = field.Value
		if scriptObject != nil {
			switch field.Type {
			case fieldtype.MAP:
				fieldMap := scriptObject.(map[string]*api.Field)
				scriptMap := createMap()
				for key, field := range fieldMap {
					v, err := s.fieldToScript(field)
					if err != nil {
						return nil, err
					}
					putInMap(scriptMap, key, v)
				}
				scriptObject = scriptMap
			case fieldtype.LIST_MAP:
				fieldListMap := scriptObject.(*linkedhashmap.Map)
				scriptMap := createMap()
				it := fieldListMap.Iterator()
				for it.HasNext() {
					entry := it.Next()
					key := entry.GetKey()
					field := entry.GetValue().(*api.Field)
					v, err := s.fieldToScript(field)
					if err != nil {
						return nil, err
					}
					scriptMap[cast.ToString(key)] = v
				}
				scriptObject = scriptMap
			case fieldtype.LIST:
				fieldArray := scriptObject.([]*api.Field)
				scripArrayElements := make([]interface{}, len(fieldArray))
				for i, field := range fieldArray {
					v, err := s.fieldToScript(field)
					if err != nil {
						return nil, err
					}
					scripArrayElements[i] = v
				}
				scriptObject = scripArrayElements

			default:
				break
			}
		}
	}
	return scriptObject, nil
}

func (s *ScriptObjectFactory) scriptToField(
	scriptObjectValue interface{},
	record api.Record,
	path string,
) (*api.Field, error) {
	if scriptObjectValue != nil {
		switch scriptObjectValue.(type) {
		case map[string]interface{}:
			scriptMap := scriptObjectValue.(map[string]interface{})
			fieldMap := make(map[string]*api.Field)
			for key, value := range scriptMap {
				valField, err := s.scriptToField(value, record, composeMapPath(path, key))
				if err != nil {
					return nil, err
				}
				fieldMap[key] = valField
			}
			return &api.Field{Type: fieldtype.MAP, Value: fieldMap}, nil
		case []bool:
			scriptList := scriptObjectValue.([]bool)
			listFieldValue := make([]*api.Field, len(scriptList))
			for index, value := range scriptList {
				valField, err := s.scriptToField(value, record, composeArrayPath(path, index))
				if err != nil {
					return nil, err
				}
				listFieldValue[index] = valField
			}
			return &api.Field{Type: fieldtype.LIST, Value: listFieldValue}, nil
		case []int8:
			scriptList := scriptObjectValue.([]int8)
			listFieldValue := make([]*api.Field, len(scriptList))
			for index, value := range scriptList {
				valField, err := s.scriptToField(value, record, composeArrayPath(path, index))
				if err != nil {
					return nil, err
				}
				listFieldValue[index] = valField
			}
			return &api.Field{Type: fieldtype.LIST, Value: listFieldValue}, nil
		case []int32:
			scriptList := scriptObjectValue.([]int32)
			listFieldValue := make([]*api.Field, len(scriptList))
			for index, value := range scriptList {
				valField, err := s.scriptToField(value, record, composeArrayPath(path, index))
				if err != nil {
					return nil, err
				}
				listFieldValue[index] = valField
			}
			return &api.Field{Type: fieldtype.LIST, Value: listFieldValue}, nil
		case []int:
			scriptList := scriptObjectValue.([]int)
			listFieldValue := make([]*api.Field, len(scriptList))
			for index, value := range scriptList {
				valField, err := s.scriptToField(value, record, composeArrayPath(path, index))
				if err != nil {
					return nil, err
				}
				listFieldValue[index] = valField
			}
			return &api.Field{Type: fieldtype.LIST, Value: listFieldValue}, nil
		case []int64:
			scriptList := scriptObjectValue.([]int64)
			listFieldValue := make([]*api.Field, len(scriptList))
			for index, value := range scriptList {
				valField, err := s.scriptToField(value, record, composeArrayPath(path, index))
				if err != nil {
					return nil, err
				}
				listFieldValue[index] = valField
			}
			return &api.Field{Type: fieldtype.LIST, Value: listFieldValue}, nil
		case []uint16:
			scriptList := scriptObjectValue.([]uint16)
			listFieldValue := make([]*api.Field, len(scriptList))
			for index, value := range scriptList {
				valField, err := s.scriptToField(value, record, composeArrayPath(path, index))
				if err != nil {
					return nil, err
				}
				listFieldValue[index] = valField
			}
			return &api.Field{Type: fieldtype.LIST, Value: listFieldValue}, nil
		case []uint32:
			scriptList := scriptObjectValue.([]uint32)
			listFieldValue := make([]*api.Field, len(scriptList))
			for index, value := range scriptList {
				valField, err := s.scriptToField(value, record, composeArrayPath(path, index))
				if err != nil {
					return nil, err
				}
				listFieldValue[index] = valField
			}
			return &api.Field{Type: fieldtype.LIST, Value: listFieldValue}, nil
		case []uint64:
			scriptList := scriptObjectValue.([]uint64)
			listFieldValue := make([]*api.Field, len(scriptList))
			for index, value := range scriptList {
				valField, err := s.scriptToField(value, record, composeArrayPath(path, index))
				if err != nil {
					return nil, err
				}
				listFieldValue[index] = valField
			}
			return &api.Field{Type: fieldtype.LIST, Value: listFieldValue}, nil
		case []float32:
			scriptList := scriptObjectValue.([]float32)
			listFieldValue := make([]*api.Field, len(scriptList))
			for index, value := range scriptList {
				valField, err := s.scriptToField(value, record, composeArrayPath(path, index))
				if err != nil {
					return nil, err
				}
				listFieldValue[index] = valField
			}
			return &api.Field{Type: fieldtype.LIST, Value: listFieldValue}, nil
		case []float64:
			scriptList := scriptObjectValue.([]float64)
			listFieldValue := make([]*api.Field, len(scriptList))
			for index, value := range scriptList {
				valField, err := s.scriptToField(value, record, composeArrayPath(path, index))
				if err != nil {
					return nil, err
				}
				listFieldValue[index] = valField
			}
			return &api.Field{Type: fieldtype.LIST, Value: listFieldValue}, nil
		case []big.Int:
			scriptList := scriptObjectValue.([]big.Int)
			listFieldValue := make([]*api.Field, len(scriptList))
			for index, value := range scriptList {
				valField, err := s.scriptToField(value, record, composeArrayPath(path, index))
				if err != nil {
					return nil, err
				}
				listFieldValue[index] = valField
			}
			return &api.Field{Type: fieldtype.LIST, Value: listFieldValue}, nil
		case []big.Float:
			scriptList := scriptObjectValue.([]big.Float)
			listFieldValue := make([]*api.Field, len(scriptList))
			for index, value := range scriptList {
				valField, err := s.scriptToField(value, record, composeArrayPath(path, index))
				if err != nil {
					return nil, err
				}
				listFieldValue[index] = valField
			}
			return &api.Field{Type: fieldtype.LIST, Value: listFieldValue}, nil
		case []string:
			scriptList := scriptObjectValue.([]string)
			listFieldValue := make([]*api.Field, len(scriptList))
			for index, value := range scriptList {
				valField, err := s.scriptToField(value, record, composeArrayPath(path, index))
				if err != nil {
					return nil, err
				}
				listFieldValue[index] = valField
			}
			return &api.Field{Type: fieldtype.LIST, Value: listFieldValue}, nil
		case []interface{}:
			scriptList := scriptObjectValue.([]interface{})
			listFieldValue := make([]*api.Field, len(scriptList))
			for index, value := range scriptList {
				valField, err := s.scriptToField(value, record, composeArrayPath(path, index))
				if err != nil {
					return nil, err
				}
				listFieldValue[index] = valField
			}
			return &api.Field{Type: fieldtype.LIST, Value: listFieldValue}, nil
		case otto.Value:
			ottoValue := scriptObjectValue.(otto.Value)
			goOttoValue, err := ottoValue.Export()
			if err != nil {
				return nil, err
			}
			return s.scriptToField(goOttoValue, record, path)
		default:
			return convertPrimitiveObject(scriptObjectValue)
		}
	} else {
		originalField, err := record.Get(path)
		if err != nil {
			return nil, err
		}
		if originalField != nil {
			return api.Create(originalField.Type, nil)
		} else {
			return api.CreateField(nil)
		}
	}
}

func (*ScriptObjectFactory) updateRecordHeader(attributes map[string]string, record api.Record) {
	for key, value := range attributes {
		record.GetHeader().SetAttribute(key, value)
	}
}

func convertPrimitiveObject(scriptObjectValue interface{}) (*api.Field, error) {
	switch scriptObjectValue.(type) {
	case bool:
		return api.CreateBoolField(scriptObjectValue.(bool))
	case []byte:
		return api.CreateByteArrayField(scriptObjectValue.([]byte))
	case byte:
		return api.CreateByteField(scriptObjectValue.(byte))
	case int8:
		return api.CreateShortField(scriptObjectValue.(int8))
	case int32:
		return api.CreateInteger32Field(scriptObjectValue.(int32))
	case int:
		return api.CreateIntegerField(scriptObjectValue.(int))
	case int64:
		return api.CreateLongField(scriptObjectValue.(int64))
	case uint16:
		return api.CreateUInteger16Field(scriptObjectValue.(uint16))
	case uint32:
		return api.CreateUInteger32Field(scriptObjectValue.(uint32))
	case uint64:
		return api.CreateLongFieldU64(scriptObjectValue.(uint64))
	case float32:
		return api.CreateFloatField(scriptObjectValue.(float32))
	case float64:
		return api.CreateDoubleField(scriptObjectValue.(float64))
	case big.Int:
		return api.CreateBigIntField(scriptObjectValue.(big.Int))
	case big.Float:
		return api.CreateBigFloatField(scriptObjectValue.(big.Float))
	case string:
		return api.CreateStringField(scriptObjectValue.(string))
	case time.Time:
		return api.CreateDateTimeField(scriptObjectValue.(time.Time))
	default:
		return GetTypedNullFieldFromScript(scriptObjectValue)
	}
}

func createMap() map[string]interface{} {
	return make(map[string]interface{})
}

func putInMap(obj interface{}, key string, value interface{}) {
	obj.(map[string]interface{})[key] = value
}

func composeMapPath(parent string, mapEntry string) string {
	return parent + "/" + mapEntry
}

func composeArrayPath(parent string, arrayIndex int) string {
	return parent + "[" + strconv.Itoa(arrayIndex) + "]"
}

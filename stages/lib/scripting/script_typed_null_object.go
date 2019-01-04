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
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/fieldtype"
)

type TypedNull struct {
	Type string
}

var NULL_BOOLEAN interface{} = &TypedNull{Type: fieldtype.BOOLEAN}
var NULL_CHAR interface{} = &TypedNull{Type: fieldtype.STRING}
var NULL_BYTE interface{} = &TypedNull{Type: fieldtype.BYTE}
var NULL_SHORT interface{} = &TypedNull{Type: fieldtype.SHORT}
var NULL_INTEGER interface{} = &TypedNull{Type: fieldtype.INTEGER}
var NULL_LONG interface{} = &TypedNull{Type: fieldtype.LONG}
var NULL_FLOAT interface{} = &TypedNull{Type: fieldtype.FLOAT}
var NULL_DOUBLE interface{} = &TypedNull{Type: fieldtype.DOUBLE}
var NULL_DECIMAL interface{} = &TypedNull{Type: fieldtype.DECIMAL}
var NULL_BYTE_ARRAY interface{} = &TypedNull{Type: fieldtype.BYTE_ARRAY}
var NULL_STRING interface{} = &TypedNull{Type: fieldtype.STRING}
var NULL_LIST interface{} = &TypedNull{Type: fieldtype.LIST}
var NULL_MAP interface{} = &TypedNull{Type: fieldtype.MAP}

func GetFieldNull(record api.Record, fieldPath string) (interface{}, error) {
	f, err := record.Get(fieldPath)
	if err != nil {
		return nil, err
	}
	if f != nil {
		if f.Value != nil {
			return f.Value, nil
		} else {
			return nil, nil
		}
	}
	return nil, err
}

func GetTypedNullFieldFromScript(scriptObject interface{}) (*api.Field, error) {
	if scriptObject == NULL_BOOLEAN {
		return api.Create(fieldtype.BOOLEAN, nil)
	} else if scriptObject == NULL_CHAR {
		return api.Create(fieldtype.STRING, nil)
	} else if scriptObject == NULL_BYTE {
		return api.Create(fieldtype.BYTE, nil)
	} else if scriptObject == NULL_SHORT {
		return api.Create(fieldtype.SHORT, nil)
	} else if scriptObject == NULL_INTEGER {
		return api.Create(fieldtype.INTEGER, nil)
	} else if scriptObject == NULL_LONG {
		return api.Create(fieldtype.LONG, nil)
	} else if scriptObject == NULL_FLOAT {
		return api.Create(fieldtype.FLOAT, nil)
	} else if scriptObject == NULL_DOUBLE {
		return api.Create(fieldtype.DOUBLE, nil)
	} else if scriptObject == NULL_DECIMAL {
		return api.Create(fieldtype.DECIMAL, nil)
	} else if scriptObject == NULL_BYTE_ARRAY {
		return api.Create(fieldtype.BYTE_ARRAY, nil)
	} else if scriptObject == NULL_STRING {
		return api.Create(fieldtype.STRING, nil)
	} else if scriptObject == NULL_LIST {
		return api.CreateListField(nil)
	} else if scriptObject == NULL_MAP {
		return api.CreateMapField(nil)
	}
	return nil, nil
}

func GetTypedNullFromField(field *api.Field) (interface{}, error) {
	switch field.Type {
	case fieldtype.BOOLEAN:
		return NULL_BOOLEAN, nil
	case fieldtype.BYTE:
		return NULL_BYTE, nil
	case fieldtype.SHORT:
		return NULL_SHORT, nil
	case fieldtype.INTEGER:
		return NULL_INTEGER, nil
	case fieldtype.LONG:
		return NULL_LONG, nil
	case fieldtype.FLOAT:
		return NULL_FLOAT, nil
	case fieldtype.DOUBLE:
		return NULL_DOUBLE, nil
	case fieldtype.DECIMAL:
		return NULL_DECIMAL, nil
	case fieldtype.BYTE_ARRAY:
		return NULL_BYTE_ARRAY, nil
	case fieldtype.STRING:
		return NULL_STRING, nil
	case fieldtype.LIST:
		return NULL_LIST, nil
	case fieldtype.MAP:
		return NULL_MAP, nil
	}
	return nil, nil
}

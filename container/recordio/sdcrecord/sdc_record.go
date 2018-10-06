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
package sdcrecord

import (
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/spf13/cast"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/fieldtype"
	"github.com/streamsets/datacollector-edge/api/linkedhashmap"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/util"
	"strconv"
	"strings"
	"time"
)

const (
	Type   = "type"
	Value  = "value"
	SqPath = "sqpath"
	DqPath = "dqpath"
)

//TODO https://issues.streamsets.com/browse/SDCE-138 Sdc Record add missing data type support

func marshalField(prefix string, f *api.Field) map[string]interface{} {
	var sdcFieldJsonValue interface{}
	switch f.Type {
	case fieldtype.LIST:
		listValue := f.Value.([]*api.Field)
		sdcRecordListValue := make([]interface{}, len(listValue))
		for i, childField := range listValue {
			sdcRecordListValue[i] = marshalField(fmt.Sprintf(prefix+"[%d]", i), childField)
		}
		sdcFieldJsonValue = sdcRecordListValue
	case fieldtype.MAP:
		mapValue := f.Value.(map[string]*api.Field)
		sdcRecordMapValue := make(map[string]interface{})
		childPrefix := prefix
		if strings.HasSuffix(prefix, "/") {
			childPrefix = strings.TrimRight(prefix, "/")
		}
		for key, childField := range mapValue {
			sdcRecordMapValue[key] = marshalField(fmt.Sprintf(childPrefix+"/%s", key), childField)
		}
		sdcFieldJsonValue = sdcRecordMapValue
	case fieldtype.LIST_MAP:
		listMapValue := f.Value.(*linkedhashmap.Map)
		sdcRecordListValue := make([]interface{}, listMapValue.Size())
		childPrefix := prefix
		if strings.HasSuffix(prefix, "/") {
			childPrefix = strings.TrimRight(prefix, "/")
		}
		i := 0
		it := listMapValue.Iterator()
		for it.HasNext() {
			entry := it.Next()
			key := entry.GetKey()
			childField := entry.GetValue().(*api.Field)
			sdcRecordListValue[i] = marshalField(fmt.Sprintf(childPrefix+"/%s", cast.ToString(key)), childField)
			i++
		}
		sdcFieldJsonValue = sdcRecordListValue
	case fieldtype.BYTE_ARRAY:
		fallthrough //Will be encoded in base64 during json serialize
	case fieldtype.BYTE:
		fallthrough
	case fieldtype.BOOLEAN:
		sdcFieldJsonValue = f.Value
	case fieldtype.DATETIME:
		sdcFieldJsonValue = fmt.Sprintf("%v", util.ConvertTimeToLong(f.Value.(time.Time)))
	default:
		//Serialize as string
		sdcFieldJsonValue = fmt.Sprintf("%v", f.Value)
	}
	return map[string]interface{}{
		Type:   f.Type,
		Value:  sdcFieldJsonValue,
		SqPath: prefix,
		DqPath: prefix,
	}
}

func unmarshalField(sdcRecordFieldJson map[string]interface{}) (*api.Field, error) {
	var err error
	var f *api.Field
	typ := sdcRecordFieldJson[Type].(string)
	value := sdcRecordFieldJson[Value]
	var stringVal string
	switch typ {
	case fieldtype.LIST:
		listValue := value.([]interface{})
		listField := make([]*api.Field, len(listValue))
		for i, listFieldElem := range listValue {
			listField[i], err = unmarshalField(listFieldElem.(map[string]interface{}))
			if err != nil {
				return nil, err
			}
		}
		if err == nil {
			f = api.CreateListFieldWithListOfFields(listField)
		}
	case fieldtype.MAP:
		mapValue := value.(map[string]interface{})
		mapField := make(map[string]*api.Field, len(mapValue))
		for k, elem := range mapValue {
			mapField[k], err = unmarshalField(elem.(map[string]interface{}))
			if err != nil {
				return nil, err
			}
		}
		if err == nil {
			f = api.CreateMapFieldWithMapOfFields(mapField)
		}
	case fieldtype.LIST_MAP:
		listMapValue := value.([]interface{})
		listMapField := linkedhashmap.New()
		for _, elem := range listMapValue {
			elemMap := elem.(map[string]interface{})
			field, err := unmarshalField(elemMap)
			if err != nil {
				return nil, err
			}
			path := cast.ToString(elemMap["sqpath"])
			listMapField.Put(util.GetLastFieldNameFromPath(path), field)
		}
		if err == nil {
			f = api.CreateListMapFieldWithMapOfFields(listMapField)
		}
	case fieldtype.BYTE_ARRAY:
		if stringBytes, ok := value.(string); ok {
			var buf []byte
			buf, err = base64.StdEncoding.DecodeString(stringBytes)
			if err == nil {
				f, err = api.CreateByteArrayField(buf)
			}
		} else {
			err = errors.New("Cannot read byte array type as String")
		}
	case fieldtype.BYTE:
		f, err = api.CreateByteField(byte(value.(float64)))
	case fieldtype.STRING:
		f, err = api.CreateStringField(value.(string))
	case fieldtype.BOOLEAN:
		f, err = api.CreateBoolField(value.(bool))
	case fieldtype.SHORT:
		stringVal = value.(string)
		var longVal int64
		if longVal, err = strconv.ParseInt(stringVal, 10, 8); err == nil {
			f, err = api.CreateShortField(int8(longVal))
		}
	case fieldtype.INTEGER:
		stringVal = value.(string)
		var longVal int64
		if longVal, err = strconv.ParseInt(stringVal, 10, 32); err == nil {
			f, err = api.CreateIntegerField(int(longVal))
		}
	case fieldtype.LONG:
		stringVal = value.(string)
		var longVal int64
		if longVal, err = strconv.ParseInt(stringVal, 10, 64); err == nil {
			f, err = api.CreateLongField(longVal)
		}
	case fieldtype.FLOAT:
		stringVal = value.(string)
		var doubleVal float64
		if doubleVal, err = strconv.ParseFloat(stringVal, 32); err == nil {
			f, err = api.CreateFloatField(float32(doubleVal))
		}
	case fieldtype.DOUBLE:
		stringVal = value.(string)
		var doubleVal float64
		if doubleVal, err = strconv.ParseFloat(stringVal, 64); err == nil {
			f, err = api.CreateDoubleField(doubleVal)
		}
	case fieldtype.DATETIME:
		stringVal = value.(string)
		var longVal int64
		if longVal, err = strconv.ParseInt(stringVal, 10, 64); err == nil {
			f, err = api.CreateDateTimeField(time.Unix(0, longVal*int64(time.Millisecond)))
		}
	}
	return f, err
}

type SDCRecord struct {
	Header *common.HeaderImpl     `json:"header"`
	Value  map[string]interface{} `json:"value"`
}

func NewSdcRecordFromRecord(r api.Record) (*SDCRecord, error) {
	var err error = nil
	var rootField *api.Field
	var sdcRecord *SDCRecord
	if rootField, err = r.Get(); err == nil {
		sdcRecord = &SDCRecord{
			Header: r.GetHeader().(*common.HeaderImpl),
			Value:  marshalField("/", rootField),
		}
	}
	return sdcRecord, err
}

func NewRecordFromSDCRecord(stageContext api.StageContext, sdcRecord *SDCRecord) (api.Record, error) {
	var err error
	var record api.Record
	if record, err = stageContext.CreateRecord(sdcRecord.Header.GetSourceId(), nil); err == nil {
		var f *api.Field
		if f, err = unmarshalField(sdcRecord.Value); err == nil {
			record.Set(f)
			originalHeaderImpl := sdcRecord.Header
			newHeaderImpl := record.GetHeader().(*common.HeaderImpl)
			//Set Headers to be same as the oldOne
			*newHeaderImpl = *originalHeaderImpl
		}
	}
	return record, err
}

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
package jsonrecord

import (
	"encoding/json"
	"github.com/spf13/cast"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/dataformats"
	"github.com/streamsets/datacollector-edge/api/fieldtype"
	"github.com/streamsets/datacollector-edge/api/linkedhashmap"
	"github.com/streamsets/datacollector-edge/container/recordio"
	"github.com/streamsets/datacollector-edge/container/util"
	"io"
	"time"
)

const (
	ArrayObjects    = "ARRAY_OBJECTS"
	MultipleObjects = "MULTIPLE_OBJECTS"
)

type JsonWriterFactoryImpl struct {
	Mode string
}

func (j *JsonWriterFactoryImpl) CreateWriter(
	context api.StageContext,
	writer io.Writer,
) (dataformats.RecordWriter, error) {
	return newRecordWriter(context, writer, j.Mode), nil
}

type JsonWriterImpl struct {
	context      api.StageContext
	writer       io.Writer
	encoder      *json.Encoder
	isArray      bool
	arrayRecords []interface{}
}

func (jsonWriter *JsonWriterImpl) WriteRecord(r api.Record) error {
	recordValue, _ := r.Get()
	jsonObject, err := writeFieldToJsonObject(recordValue)
	if err != nil {
		return err
	}
	if jsonWriter.isArray {
		jsonWriter.arrayRecords = append(jsonWriter.arrayRecords, jsonObject)
	} else {
		jsonWriter.encoder.Encode(jsonObject)
	}
	return nil
}

func (jsonWriter *JsonWriterImpl) Flush() error {
	return recordio.Flush(jsonWriter.writer)
}

func (jsonWriter *JsonWriterImpl) Close() error {
	if jsonWriter.isArray {
		jsonWriter.encoder.Encode(jsonWriter.arrayRecords)
		recordio.Flush(jsonWriter.writer)
	}
	return recordio.Close(jsonWriter.writer)
}

func newRecordWriter(context api.StageContext, writer io.Writer, mode string) *JsonWriterImpl {
	jsonWriter := &JsonWriterImpl{
		context: context,
		writer:  writer,
		encoder: json.NewEncoder(writer),
		isArray: mode == ArrayObjects,
	}
	if jsonWriter.isArray {
		jsonWriter.arrayRecords = make([]interface{}, 0)
	}
	return jsonWriter
}

func writeFieldToJsonObject(field *api.Field) (interface{}, error) {
	if field.Value == nil {
		return nil, nil
	}
	var err error = nil
	switch field.Type {
	case fieldtype.LIST:
		jsonObject := make([]interface{}, 0)
		fieldValue := field.Value.([]*api.Field)
		for _, v := range fieldValue {
			fieldJsonObject, err := writeFieldToJsonObject(v)
			if err != nil {
				return nil, err
			}
			jsonObject = append(jsonObject, fieldJsonObject)
		}
		return jsonObject, err
	case fieldtype.MAP:
		jsonObject := make(map[string]interface{})
		fieldValue := field.Value.(map[string]*api.Field)
		for k, v := range fieldValue {
			jsonObject[k], err = writeFieldToJsonObject(v)
			if err != nil {
				return nil, err
			}
		}
		return jsonObject, err
	case fieldtype.LIST_MAP:
		jsonObject := make(map[string]interface{})
		listMapValue := field.Value.(*linkedhashmap.Map)
		it := listMapValue.Iterator()
		for it.HasNext() {
			entry := it.Next()
			key := entry.GetKey()
			value := entry.GetValue().(*api.Field)
			jsonObject[cast.ToString(key)], err = writeFieldToJsonObject(value)
			if err != nil {
				return nil, err
			}
		}
		return jsonObject, err
	case fieldtype.DATETIME:
		return util.ConvertTimeToLong(field.Value.(time.Time)), nil
	default:
		return field.Value, nil
	}
}

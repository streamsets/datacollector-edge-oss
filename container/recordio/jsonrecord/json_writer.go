/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jsonrecord

import (
	"encoding/json"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/fieldtype"
	"github.com/streamsets/datacollector-edge/container/recordio"
	"io"
)

type JsonWriterFactoryImpl struct {
	// TODO: Add needed configs
}

func (j *JsonWriterFactoryImpl) CreateWriter(
	context api.StageContext,
	writer io.Writer,
) (recordio.RecordWriter, error) {
	var recordWriter recordio.RecordWriter
	recordWriter = newRecordWriter(context, writer)
	return recordWriter, nil
}

type JsonWriterImpl struct {
	context api.StageContext
	writer  io.Writer
	encoder *json.Encoder
}

func (jsonWriter *JsonWriterImpl) WriteRecord(r api.Record) error {
	recordValue, _ := r.Get()
	jsonObject, err := writeFieldToJsonObject(recordValue)
	if err != nil {
		return err
	}
	jsonWriter.encoder.Encode(jsonObject)
	return nil
}

func (jsonWriter *JsonWriterImpl) Flush() error {
	return recordio.Flush(jsonWriter.writer)
}

func (jsonWriter *JsonWriterImpl) Close() error {
	return recordio.Close(jsonWriter.writer)
}

func newRecordWriter(context api.StageContext, writer io.Writer) *JsonWriterImpl {
	return &JsonWriterImpl{
		context: context,
		writer:  writer,
		encoder: json.NewEncoder(writer),
	}
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
	default:
		return field.Value, nil
	}
}

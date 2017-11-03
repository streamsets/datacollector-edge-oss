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
	"github.com/streamsets/datacollector-edge/container/recordio"
	"io"
)

type JsonReaderFactoryImpl struct {
	// TODO: Add needed configs
}

func (j *JsonReaderFactoryImpl) CreateReader(
	context api.StageContext,
	reader io.Reader,
) (recordio.RecordReader, error) {
	var recordReader recordio.RecordReader
	recordReader = newRecordReader(context, reader)
	return recordReader, nil
}

type JsonReaderImpl struct {
	context api.StageContext
	reader  io.Reader
	decoder *json.Decoder
}

func (jsonReader *JsonReaderImpl) ReadRecord() (api.Record, error) {
	var f interface{}
	err := jsonReader.decoder.Decode(&f)
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}
	return jsonReader.context.CreateRecord("sourceId", f)
}

func (jsonReader *JsonReaderImpl) Close() error {
	return recordio.Close(jsonReader.reader)
}

func newRecordReader(context api.StageContext, reader io.Reader) *JsonReaderImpl {
	return &JsonReaderImpl{
		context: context,
		reader:  reader,
		decoder: json.NewDecoder(reader),
	}
}

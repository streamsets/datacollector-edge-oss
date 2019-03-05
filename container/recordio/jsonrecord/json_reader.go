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
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/dataformats"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/recordio"
	"io"
)

type JsonReaderFactoryImpl struct {
	recordio.AbstractRecordReaderFactory
	// TODO: Add needed configs
}

func (j *JsonReaderFactoryImpl) CreateReader(
	context api.StageContext,
	reader io.Reader,
	messageId string,
) (dataformats.RecordReader, error) {
	var recordReader dataformats.RecordReader
	recordReader = newRecordReader(context, reader, messageId)
	return recordReader, nil
}

type JsonReaderImpl struct {
	context   api.StageContext
	reader    io.Reader
	decoder   *json.Decoder
	messageId string
	counter   int
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
	jsonReader.counter++
	sourceId := common.CreateRecordId(jsonReader.messageId, jsonReader.counter)
	return jsonReader.context.CreateRecord(sourceId, f)
}

func (jsonReader *JsonReaderImpl) Close() error {
	return recordio.Close(jsonReader.reader)
}

func newRecordReader(context api.StageContext, reader io.Reader, messageId string) *JsonReaderImpl {
	return &JsonReaderImpl{
		context:   context,
		reader:    reader,
		decoder:   json.NewDecoder(reader),
		messageId: messageId,
		counter:   0,
	}
}

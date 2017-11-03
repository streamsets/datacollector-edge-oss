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
package textrecord

import (
	"bufio"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/recordio"
	"io"
	"strings"
)

type TextReaderFactoryImpl struct {
	// TODO: Add needed configs
}

func (j *TextReaderFactoryImpl) CreateReader(
	context api.StageContext,
	reader io.Reader,
) (recordio.RecordReader, error) {
	var recordReader recordio.RecordReader
	recordReader = newRecordReader(context, reader)
	return recordReader, nil
}

type TextReaderImpl struct {
	context api.StageContext
	reader  *bufio.Reader
}

func (textReader *TextReaderImpl) ReadRecord() (api.Record, error) {
	var err error
	line, err := textReader.reader.ReadString('\n')
	if err != nil && err != io.EOF {
		return nil, err
	}
	if len(line) > 0 {
		recordValue := map[string]interface{}{"text": strings.Replace(line, "\n", "", 1)}
		return textReader.context.CreateRecord("sourceId", recordValue)
	}
	return nil, nil
}

func (textReader *TextReaderImpl) Close() error {
	return recordio.Close(textReader.reader)
}

func newRecordReader(context api.StageContext, reader io.Reader) *TextReaderImpl {
	return &TextReaderImpl{
		context: context,
		reader:  bufio.NewReader(reader),
	}
}

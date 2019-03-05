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
package delimitedrecord

import (
	"encoding/csv"
	"fmt"
	"github.com/spf13/cast"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/dataformats"
	"github.com/streamsets/datacollector-edge/api/linkedhashmap"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/recordio"
	"io"
)

const (
	List         = "LIST"
	ListMap      = "LIST_MAP"
	WithHeader   = "WITH_HEADER"
	IgnoreHeader = "IGNORE_HEADER"
	NoHeader     = "NO_HEADER"
	Custom       = "CUSTOM"
)

type DelimitedReaderFactoryImpl struct {
	recordio.AbstractRecordReaderFactory
	CsvFileFormat        string
	CsvHeader            string
	CsvAllowExtraColumns bool
	CsvExtraColumnPrefix string
	CsvMaxObjectLen      float64
	CsvCustomDelimiter   string
	CsvCustomEscape      string
	CsvEnableComments    bool
	CsvCommentMarker     string
	CsvIgnoreEmptyLines  bool
	CsvRecordType        string
	CsvSkipStartLines    float64
	ParseNull            bool
	NullConstant         string
}

func (j *DelimitedReaderFactoryImpl) CreateReader(
	context api.StageContext,
	reader io.Reader,
	messageId string,
) (dataformats.RecordReader, error) {
	comma := ','
	if j.CsvFileFormat == Custom && len(j.CsvCustomDelimiter) > 0 {
		runeArr := []rune(j.CsvCustomDelimiter)
		comma = runeArr[0]
	}
	recordReader := newRecordReader(context, reader, messageId)
	recordReader.reader.ReuseRecord = true
	recordReader.reader.Comma = comma
	recordReader.recordType = j.CsvRecordType
	recordReader.header = j.CsvHeader
	recordReader.skipStartLines = int(j.CsvSkipStartLines)
	return recordReader, nil
}

type DelimitedReaderImpl struct {
	context        api.StageContext
	reader         *csv.Reader
	messageId      string
	counter        int
	recordType     string
	skipStartLines int
	header         string
	headers        []*api.Field
	sep            string
}

func (delimitedReader *DelimitedReaderImpl) ReadRecord() (api.Record, error) {
	var err error
	columns, err := delimitedReader.reader.Read()
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}
	delimitedReader.counter++
	return delimitedReader.CreateRecord(columns)
}

func (delimitedReader *DelimitedReaderImpl) CreateRecord(columns []string) (api.Record, error) {
	// handle CSV Header
	if delimitedReader.counter == 1 && len(delimitedReader.headers) == 0 {
		switch delimitedReader.header {
		case WithHeader:
			delimitedReader.headers = make([]*api.Field, len(columns))
			for i, col := range columns {
				headerField, _ := api.CreateStringField(col)
				delimitedReader.headers[i] = headerField
			}
			return delimitedReader.ReadRecord()
		case IgnoreHeader:
			// ignore header line
			return delimitedReader.ReadRecord()
		case NoHeader:
			// Do nothing
		default:
			return nil, fmt.Errorf("invalid CSV Header type")
		}
	}

	if delimitedReader.counter <= delimitedReader.skipStartLines {
		return delimitedReader.ReadRecord()
	}

	return createRecord(
		delimitedReader.context,
		delimitedReader.messageId,
		delimitedReader.counter,
		delimitedReader.recordType,
		columns,
		delimitedReader.headers,
	)
}

func (delimitedReader *DelimitedReaderImpl) Close() error {
	return recordio.Close(delimitedReader.reader)
}

func newRecordReader(context api.StageContext, reader io.Reader, messageId string) *DelimitedReaderImpl {
	return &DelimitedReaderImpl{
		context:   context,
		reader:    csv.NewReader(reader),
		messageId: messageId,
		counter:   0,
	}
}

func createRecord(
	context api.StageContext,
	messageId string,
	counter int,
	recordType string,
	columns []string,
	headers []*api.Field,
) (api.Record, error) {
	sourceId := common.CreateRecordId(messageId, counter)
	if recordType == List {
		recordVal := make([]*api.Field, len(columns))
		for i, col := range columns {
			cellField := make(map[string]*api.Field)
			if i < len(headers) {
				cellField["header"] = headers[i]
			}
			cellField["value"], _ = api.CreateStringField(col)
			recordVal[i] = api.CreateMapFieldWithMapOfFields(cellField)
		}
		record, err := context.CreateRecord(sourceId, nil)
		if err != nil {
			return nil, err
		}
		record.Set(api.CreateListFieldWithListOfFields(recordVal))
		return record, err
	} else if recordType == ListMap {
		recordVal := linkedhashmap.New()
		for i, col := range columns {
			key := cast.ToString(i)
			if i < len(headers) {
				key = cast.ToString(headers[i].Value)
			}
			colField, _ := api.CreateStringField(col)
			recordVal.Put(key, colField)
		}
		record, err := context.CreateRecord(sourceId, nil)
		if err != nil {
			return nil, err
		}
		record.Set(api.CreateListMapFieldWithMapOfFields(recordVal))
		return record, err
	} else {
		return nil, fmt.Errorf("invalid Record type: %s", recordType)
	}
}

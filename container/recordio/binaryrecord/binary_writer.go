// Copyright 2019 StreamSets Inc.
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
package binaryrecord

import (
	"bufio"
	"fmt"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/dataformats"
	"github.com/streamsets/datacollector-edge/api/fieldtype"
	"github.com/streamsets/datacollector-edge/container/recordio"
	"io"
)

const DefaultBinaryFieldPath = "/"

type BinaryWriterFactoryImpl struct {
	BinaryFieldPath string
}

func (t *BinaryWriterFactoryImpl) CreateWriter(
	context api.StageContext,
	writer io.Writer,
) (dataformats.RecordWriter, error) {
	var recordWriter dataformats.RecordWriter
	recordWriter = newRecordWriter(context, writer, t.BinaryFieldPath)
	return recordWriter, nil
}

type BinaryWriterImpl struct {
	context         api.StageContext
	writer          *bufio.Writer
	binaryFieldPath string
}

func (binaryWriter *BinaryWriterImpl) WriteRecord(r api.Record) error {
	if binaryValue, err := r.Get(binaryWriter.binaryFieldPath); err != nil {
		return err
	} else if binaryValue.Type != fieldtype.BYTE_ARRAY {
		return fmt.Errorf("invalid data type %s for binary writer", binaryValue.Type)
	} else if _, err := binaryWriter.writer.Write(binaryValue.Value.([]byte)); err != nil {
		return err
	}
	return nil
}

func (binaryWriter *BinaryWriterImpl) Flush() error {
	return recordio.Flush(binaryWriter.writer)
}

func (binaryWriter *BinaryWriterImpl) Close() error {
	return recordio.Close(binaryWriter.writer)
}

func newRecordWriter(
	context api.StageContext,
	writer io.Writer,
	binaryFieldPath string,
) *BinaryWriterImpl {
	if len(binaryFieldPath) == 0 {
		binaryFieldPath = DefaultBinaryFieldPath
	}
	return &BinaryWriterImpl{
		context:         context,
		writer:          bufio.NewWriter(writer),
		binaryFieldPath: binaryFieldPath,
	}
}

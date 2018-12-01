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
package textrecord

import (
	"bufio"
	"fmt"
	"github.com/spf13/cast"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/dataformats"
	"github.com/streamsets/datacollector-edge/container/recordio"
	"io"
)

type TextWriterFactoryImpl struct {
	TextFieldPath string
}

func (t *TextWriterFactoryImpl) CreateWriter(
	context api.StageContext,
	writer io.Writer,
) (dataformats.RecordWriter, error) {
	var recordWriter dataformats.RecordWriter
	recordWriter = newRecordWriter(context, writer, t.TextFieldPath)
	return recordWriter, nil
}

type TextWriterImpl struct {
	context       api.StageContext
	writer        *bufio.Writer
	textFieldPath string
}

func (textWriter *TextWriterImpl) WriteRecord(r api.Record) error {
	if textFieldValue, err := r.Get(textWriter.textFieldPath); err != nil {
		return err
	} else if _, err = fmt.Fprintln(textWriter.writer, cast.ToString(textFieldValue.Value)); err != nil {
		return err
	}
	return nil
}

func (textWriter *TextWriterImpl) Flush() error {
	return recordio.Flush(textWriter.writer)
}

func (textWriter *TextWriterImpl) Close() error {
	return recordio.Close(textWriter.writer)
}

func newRecordWriter(
	context api.StageContext,
	writer io.Writer,
	textFieldPath string,
) *TextWriterImpl {
	if len(textFieldPath) == 0 {
		textFieldPath = DefaultTextFieldPath
	}
	return &TextWriterImpl{
		context:       context,
		writer:        bufio.NewWriter(writer),
		textFieldPath: textFieldPath,
	}
}

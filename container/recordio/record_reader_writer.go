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
package recordio

import (
	"errors"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/dataformats"
	"io"
)

const (
	CompressedNone = "NONE"
	CompressedFile = "COMPRESSED_FILE"
)

type RecordReaderFactory interface {
	CreateReader(context api.StageContext, reader io.Reader, messageId string) (dataformats.RecordReader, error)
	CreateWholeFileReader(
		context api.StageContext,
		messageId string,
		metadata map[string]interface{},
		fileRef api.FileRef,
	) (dataformats.RecordReader, error)
}

type AbstractRecordReaderFactory struct {
}

func (*AbstractRecordReaderFactory) CreateWholeFileReader(
	context api.StageContext,
	messageId string,
	metadata map[string]interface{},
	fileRef api.FileRef,
) (dataformats.RecordReader, error) {
	return nil, errors.New("not supported operation")
}

func (*AbstractRecordReaderFactory) CreateReader(
	context api.StageContext,
	reader io.Reader,
	messageId string,
) (dataformats.RecordReader, error) {
	return nil, errors.New("not supported operation")
}

type RecordWriterFactory interface {
	CreateWriter(context api.StageContext, writer io.Writer) (dataformats.RecordWriter, error)
}

type Flusher interface {
	Flush() error
}

func Flush(v interface{}) error {
	c, ok := v.(Flusher)
	if ok {
		return c.Flush()
	}
	return nil
}

func Close(v interface{}) error {
	c, ok := v.(io.Closer)
	if ok {
		return c.Close()
	}
	return nil
}

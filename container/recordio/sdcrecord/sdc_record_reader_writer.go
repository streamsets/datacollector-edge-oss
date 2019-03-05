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
	"encoding/json"
	"errors"
	"fmt"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/dataformats"
	"github.com/streamsets/datacollector-edge/container/recordio"
	"io"
)

const (
	//Just support Json For now
	SdcJsonMagicNumber = byte(0xa0) | byte(0x01)
)

var NewLineBytes = []byte("\n")

type SDCRecordReaderFactoryImpl struct {
	recordio.AbstractRecordReaderFactory
	//TODO: Add needed configs
}

func (srrf *SDCRecordReaderFactoryImpl) CreateReader(
	context api.StageContext,
	reader io.Reader,
	messageId string,
) (dataformats.RecordReader, error) {
	var recordReader dataformats.RecordReader
	b := make([]byte, 1)
	//Magic number read.
	nb, err := reader.Read(b)
	if err == nil {
		if nb != 1 {
			err = errors.New(
				fmt.Sprintf(
					"Error Creating Reader, when reading magic byte."+
						" Read : %x, number of bytes: %d",
					b,
					nb,
				),
			)
		} else if b[0] != SdcJsonMagicNumber {
			err = errors.New("Error Creating Reader: Magic number does not point to JSON")
		}
		recordReader = newRecordReader(context, reader)
	}
	return recordReader, err
}

type SDCRecordWriterFactoryImpl struct {
	//TODO: Add needed configs
}

func (srwf *SDCRecordWriterFactoryImpl) CreateWriter(
	context api.StageContext,
	writer io.Writer,
) (dataformats.RecordWriter, error) {
	var recordWriter dataformats.RecordWriter

	//Magic Number for SDC record
	_, err := writer.Write([]byte{SdcJsonMagicNumber})

	if err == nil {
		recordWriter = newRecordWriter(context, writer)
	}

	return recordWriter, err
}

type SDCRecordReaderImpl struct {
	context api.StageContext
	reader  io.Reader
	decoder *json.Decoder
}

func (srr *SDCRecordReaderImpl) ReadRecord() (api.Record, error) {
	sdcRecord := new(SDCRecord)
	err := srr.decoder.Decode(sdcRecord)
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}
	return NewRecordFromSDCRecord(srr.context, sdcRecord)
}

func (srr *SDCRecordReaderImpl) Close() error {
	return recordio.Close(srr.reader)
}

type SDCRecordWriterImpl struct {
	context api.StageContext
	writer  io.Writer
	encoder *json.Encoder
}

func (srw *SDCRecordWriterImpl) WriteRecord(r api.Record) error {
	sdcRecord, err := NewSdcRecordFromRecord(r)
	if err == nil {
		err = srw.encoder.Encode(*sdcRecord)
	}
	srw.writer.Write(NewLineBytes)
	return err
}

func (srw *SDCRecordWriterImpl) Flush() error {
	return recordio.Flush(srw.writer)
}

func (srw *SDCRecordWriterImpl) Close() error {
	return recordio.Close(srw.writer)
}

func newRecordWriter(context api.StageContext, writer io.Writer) *SDCRecordWriterImpl {
	return &SDCRecordWriterImpl{
		context: context,
		writer:  writer,
		encoder: json.NewEncoder(writer),
	}
}

func newRecordReader(context api.StageContext, reader io.Reader) *SDCRecordReaderImpl {
	return &SDCRecordReaderImpl{
		context: context,
		reader:  reader,
		decoder: json.NewDecoder(reader),
	}
}

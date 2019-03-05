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
	"compress/gzip"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/dataformats"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/recordio"
	"io"
)

type BinaryReaderFactoryImpl struct {
	recordio.AbstractRecordReaderFactory
	BinaryMaxObjectLen int
	Compression        string
}

func (b *BinaryReaderFactoryImpl) CreateReader(
	context api.StageContext,
	reader io.Reader,
	messageId string,
) (dataformats.RecordReader, error) {
	return newRecordReader(context, reader, messageId, b.BinaryMaxObjectLen, b.Compression)
}

type BinaryReaderImpl struct {
	context      api.StageContext
	reader       io.Reader
	maxObjectLen int
	messageId    string
	counter      int
}

func (binaryReader *BinaryReaderImpl) ReadRecord() (api.Record, error) {
	var err error
	bytes := make([]byte, binaryReader.maxObjectLen)
	n, err := binaryReader.reader.Read(bytes)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n > 0 {
		binaryReader.counter++
		sourceId := common.CreateRecordId(binaryReader.messageId, binaryReader.counter)
		return binaryReader.context.CreateRecord(sourceId, bytes[:n])
	}

	return nil, nil
}

func (binaryReader *BinaryReaderImpl) Close() error {
	return recordio.Close(binaryReader.reader)
}

func newRecordReader(
	context api.StageContext,
	reader io.Reader,
	messageId string,
	maxObjectLen int,
	compression string,
) (*BinaryReaderImpl, error) {
	if compression == recordio.CompressedFile {
		var err error
		reader, err = gzip.NewReader(reader)
		if err != nil {
			return nil, err
		}
	}
	return &BinaryReaderImpl{
		context:      context,
		reader:       reader,
		maxObjectLen: maxObjectLen,
		messageId:    messageId,
		counter:      0,
	}, nil
}

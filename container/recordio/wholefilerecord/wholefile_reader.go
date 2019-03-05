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
package wholefilerecord

import (
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/dataformats"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/recordio"
)

type WholeFileReaderFactoryImpl struct {
	recordio.AbstractRecordReaderFactory
	WholeFileMaxObjectLen int
	RateLimit             string
	VerifyChecksum        bool
}

func (w *WholeFileReaderFactoryImpl) CreateWholeFileReader(
	context api.StageContext,
	messageId string,
	metadata map[string]interface{},
	fileRef api.FileRef,
) (dataformats.RecordReader, error) {
	return newRecordReader(context, messageId, metadata, fileRef)
}

type WholeFileReaderImpl struct {
	context   api.StageContext
	messageId string
	metadata  map[string]interface{}
	fileRef   api.FileRef
}

func (wholeFileReader *WholeFileReaderImpl) ReadRecord() (api.Record, error) {
	recordValue := map[string]interface{}{
		FileRefFieldName:  wholeFileReader.fileRef,
		FileInfoFieldName: wholeFileReader.metadata,
	}
	sourceId := common.CreateRecordId(wholeFileReader.messageId, 1)
	return wholeFileReader.context.CreateRecord(sourceId, recordValue)
}

func (wholeFileReader *WholeFileReaderImpl) Close() error {
	return nil
}

func newRecordReader(
	context api.StageContext,
	messageId string,
	metadata map[string]interface{},
	fileRef api.FileRef,
) (*WholeFileReaderImpl, error) {
	return &WholeFileReaderImpl{
		context:   context,
		messageId: messageId,
		metadata:  metadata,
		fileRef:   fileRef,
	}, nil
}

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
	"bytes"
	"compress/gzip"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/fieldtype"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/recordio"
	"testing"
)

func CreateStageContext() api.StageContext {
	return &common.StageContextImpl{
		StageConfig: &common.StageConfiguration{InstanceName: "Dummy Stage"},
		Parameters:  nil,
	}
}

func TestReadBinaryRecord(t *testing.T) {
	sampleText := "Sample data for binary data parser test"
	sampleTextBinary := []byte(sampleText)
	sampleBinaryData := bytes.NewBuffer(sampleTextBinary)
	stageContext := CreateStageContext()
	readerFactoryImpl := &BinaryReaderFactoryImpl{BinaryMaxObjectLen: 1024}
	recordReader, err := readerFactoryImpl.CreateReader(stageContext, sampleBinaryData, "m")
	if err != nil {
		t.Fatal(err.Error())
	}

	recordCount := 0
	for {
		record, err := recordReader.ReadRecord()
		if err != nil {
			t.Fatal(err.Error())
		}

		if record == nil {
			break
		}

		rootField, _ := record.Get()
		if rootField.Type != fieldtype.BYTE_ARRAY {
			t.Errorf("Excpeted record type : BYTE_ARRAY, but received: %s", rootField.Type)
		}

		recordValue := rootField.Value.([]byte)

		if string(sampleTextBinary) != string(recordValue) {
			t.Errorf("Excpeted record value : %s, but received: %s", string(sampleTextBinary), string(recordValue))
		}

		recordCount++
	}

	if recordCount != 1 {
		t.Errorf("Excpeted 1 records, but received: %d", recordCount)
	}

	_ = recordReader.Close()
}

func TestReadBinaryRecordMaxObjectLen(t *testing.T) {
	sampleText := "Sample data for binary data parser test"
	sampleTextBinary := []byte(sampleText)
	sampleBinaryData := bytes.NewBuffer(sampleTextBinary)
	stageContext := CreateStageContext()
	readerFactoryImpl := &BinaryReaderFactoryImpl{BinaryMaxObjectLen: 20}
	recordReader, err := readerFactoryImpl.CreateReader(stageContext, sampleBinaryData, "m")
	if err != nil {
		t.Fatal(err.Error())
	}

	recordCount := 0
	for {
		record, err := recordReader.ReadRecord()
		if err != nil {
			t.Fatal(err.Error())
		}

		if record == nil {
			break
		}
		recordCount++
	}

	if recordCount != 2 {
		t.Errorf("Excpeted 2 records, but received: %d", recordCount)
	}

	_ = recordReader.Close()
}

func TestReadBinaryRecordCompression(t *testing.T) {
	sampleText := "Sample data for binary data parser test"
	sampleTextBinary := []byte(sampleText)

	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	if _, err := gz.Write(sampleTextBinary); err != nil {
		t.Error(err)
	}
	if err := gz.Flush(); err != nil {
		t.Error(err)
	}
	if err := gz.Close(); err != nil {
		t.Error(err)
	}

	sampleBinaryCompressedData := bytes.NewBuffer(b.Bytes())

	stageContext := CreateStageContext()
	readerFactoryImpl := &BinaryReaderFactoryImpl{BinaryMaxObjectLen: 1024, Compression: recordio.CompressedFile}
	recordReader, err := readerFactoryImpl.CreateReader(stageContext, sampleBinaryCompressedData, "m")
	if err != nil {
		t.Fatal(err.Error())
	}

	recordCount := 0
	for {
		record, err := recordReader.ReadRecord()
		if err != nil {
			t.Fatal(err.Error())
		}

		if record == nil {
			break
		}

		rootField, _ := record.Get()
		if rootField.Type != fieldtype.BYTE_ARRAY {
			t.Errorf("Excpeted record type : BYTE_ARRAY, but received: %s", rootField.Type)
		}

		recordValue := rootField.Value.([]byte)

		if string(sampleTextBinary) != string(recordValue) {
			t.Errorf("Excpeted record value : %s, but received: %s", string(sampleTextBinary), string(recordValue))
		}

		recordCount++
	}

	if recordCount != 1 {
		t.Errorf("Excpeted 1 records, but received: %d", recordCount)
	}

	_ = recordReader.Close()
}

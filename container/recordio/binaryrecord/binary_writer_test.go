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
	"testing"
)

func TestWriteBinaryRecord(t *testing.T) {
	testWriteBinaryRecord(t, "")
	testWriteBinaryRecord(t, "newBinaryFieldName")
}

func testWriteBinaryRecord(t *testing.T, binaryFieldName string) {
	stageContext := CreateStageContext()
	record1, err := stageContext.CreateRecord("Id1", getBinaryFieldRecord(binaryFieldName, "log line 1"))
	record2, err := stageContext.CreateRecord("Id2", getBinaryFieldRecord(binaryFieldName, "log line 2"))

	bufferWriter := bytes.NewBuffer([]byte{})
	recordWriterFactory := &BinaryWriterFactoryImpl{BinaryFieldPath: "/" + binaryFieldName}
	recordWriter, err := recordWriterFactory.CreateWriter(stageContext, bufferWriter)
	if err != nil {
		t.Fatal(err)
	}

	err = recordWriter.WriteRecord(record1)
	if err != nil {
		t.Fatal(err)
	}

	err = recordWriter.WriteRecord(record2)
	if err != nil {
		t.Fatal(err)
	}

	_ = recordWriter.Flush()
	_ = recordWriter.Close()

	testData := "log line 1log line 2"
	if bufferWriter.String() != "log line 1log line 2" {
		t.Errorf("Excpeted field value %s, but received: %s", testData, bufferWriter.String())
	}
}

func getBinaryFieldRecord(binaryFieldName string, val string) interface{} {
	if len(binaryFieldName) == 0 {
		return []byte(val)
	}
	return map[string]interface{}{binaryFieldName: []byte(val)}
}

func TestWriteBinaryRecordInvalidFieldPath(t *testing.T) {
	stageContext := CreateStageContext()
	record1, err := stageContext.CreateRecord("Id1", getBinaryFieldRecord("", "log line 1"))

	bufferWriter := bytes.NewBuffer([]byte{})
	recordWriterFactory := &BinaryWriterFactoryImpl{BinaryFieldPath: "/invalidFieldPath"}
	recordWriter, err := recordWriterFactory.CreateWriter(stageContext, bufferWriter)
	if err != nil {
		t.Fatal(err)
	}

	err = recordWriter.WriteRecord(record1)
	if err == nil {
		t.Fatal("expected error")
	}
}

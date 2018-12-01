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
	"bytes"
	"github.com/streamsets/datacollector-edge/api/linkedhashmap"
	"testing"
)

func TestWriteTextRecord(t *testing.T) {
	testWriteTextRecord(t, DefaultTextField)
	testWriteTextRecord(t, "newTextFieldName")
}

func testWriteTextRecord(t *testing.T, textFieldName string) {
	stageContext := CreateStageContext()
	record1, err := stageContext.CreateRecord("Id1", map[string]interface{}{textFieldName: "log line 1"})
	record2, err := stageContext.CreateRecord("Id2", map[string]interface{}{textFieldName: "log line 2"})
	listMapValue := linkedhashmap.New()
	listMapValue.Put(textFieldName, "log line 3")
	record3, err := stageContext.CreateRecord("Id3", listMapValue)

	bufferWriter := bytes.NewBuffer([]byte{})
	recordWriterFactory := &TextWriterFactoryImpl{TextFieldPath: "/" + textFieldName}
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

	err = recordWriter.WriteRecord(record3)
	if err != nil {
		t.Fatal(err)
	}

	_ = recordWriter.Flush()
	_ = recordWriter.Close()

	testData := "log line 1\nlog line 2\nlog line 3\n"
	if bufferWriter.String() != "log line 1\nlog line 2\nlog line 3\n" {
		t.Errorf("Excpeted field value %s, but received: %s", testData, bufferWriter.String())
	}
}

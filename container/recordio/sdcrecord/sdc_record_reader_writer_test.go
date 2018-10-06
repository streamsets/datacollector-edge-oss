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
	"bytes"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/fieldtype"
	"github.com/streamsets/datacollector-edge/api/linkedhashmap"
	"github.com/streamsets/datacollector-edge/container/common"
	"reflect"
	"strings"
	"testing"
)

const (
	JSON1 = "{\"header\":{\"stageCreator\":\"Dummy Stage\",\"sourceId\":\"Sample Record Id1\"," +
		"\"stagesPath\":\"\",\"trackingId\":\"\",\"previousTrackingId\":\"\"," +
		"\"errorDataCollectorId\":\"\",\"errorPipelineName\":\"\",\"errorStage\":\"\"," +
		"\"errorMessage\":\"\",\"errorTimestamp\":0,\"values\":{\"Sample Attribute\":\"Sample Value1\"}}," +
		"\"value\":{\"type\":\"STRING\",\"value\":\"Sample Data1\",\"sqpath\":\"/\",\"dqpath\":\"/\"}}"
	JSON2 = "{\"header\":{\"stageCreator\":\"Dummy Stage\",\"sourceId\":\"Sample Record Id2\"," +
		"\"stagesPath\":\"\",\"trackingId\":\"\",\"previousTrackingId\":\"\"," +
		"\"errorDataCollectorId\":\"\",\"errorPipelineName\":\"\",\"errorStage\":\"\"," +
		"\"errorMessage\":\"\",\"errorTimestamp\":0,\"values\":{\"Sample Attribute\":\"Sample Value2\"}}," +
		"\"value\":{\"dqpath\":\"/\",\"sqpath\":\"/\",\"type\":\"MAP\",\"value\":{\"sampleListField\":" +
		"{\"dqpath\":\"/sampleListField\",\"sqpath\":\"/sampleListField\",\"type\":\"LIST\",\"value\":" +
		"[{\"dqpath\":\"/sampleListField[0]\",\"sqpath\":\"/sampleListField[0]\",\"type\":\"STRING\",\"value\":\"a\"}," +
		"{\"dqpath\":\"/sampleListField[1]\",\"sqpath\":\"/sampleListField[1]\",\"type\":\"STRING\",\"value\":\"b\"}]}," +
		"\"sampleMapField\":{\"dqpath\":\"/sampleMapField\",\"sqpath\":\"/sampleMapField\",\"type\":\"MAP\"," +
		"\"value\":{\"a\":{\"dqpath\":\"/sampleMapField/a\",\"sqpath\":\"/sampleMapField/a\",\"type\":\"INTEGER\"," +
		"\"value\":\"1\"},\"b\":{\"dqpath\":\"/sampleMapField/b\",\"sqpath\":\"/sampleMapField/b\",\"type\":\"INTEGER\"," +
		"\"value\":\"2\"}}},\"sampleStringField\":{\"dqpath\":\"/sampleStringField\",\"sqpath\":\"/sampleStringField\"," +
		"\"type\":\"STRING\",\"value\":\"abc\"}}}}"
)

var JSON = string([]byte{SdcJsonMagicNumber}) + JSON1 + JSON2
var complexRecordField = map[string]interface{}{
	"sampleMapField":    map[string]interface{}{"a": 1, "b": 2},
	"sampleListField":   []string{"a", "b"},
	"sampleStringField": "abc",
}

func getAllTypesRecordField() map[string]interface{} {
	sampleListMap := linkedhashmap.New()
	sampleListMap.Put("a", 1)
	sampleListMap.Put("b", 2)
	return map[string]interface{}{
		"sampleBool":       true,
		"sampleByte":       byte(0xa1),
		"sampleByteArray":  []byte{0xa0, 0xb1, 0xc2, 0xd3},
		"sampleShort":      int8(1),
		"sampleInteger":    int(2),
		"sampleLong":       int64(3),
		"sampleFloat":      float32(1.0),
		"sampleDouble":     float64(2.0),
		"sampleString":     "sample",
		"sampleMap":        map[string]interface{}{"a": 1, "b": 2},
		"sampleStringList": []string{"a", "b"},
		"sampleList":       []interface{}{1, 2},
		"sampleListMap":    sampleListMap,
	}
}

func CreateStageContext() api.StageContext {
	return &common.StageContextImpl{
		StageConfig: &common.StageConfiguration{InstanceName: "Dummy Stage"},
		Parameters:  nil,
	}
}

func TestWriteRecord(t *testing.T) {
	st := CreateStageContext()
	record1, _ := st.CreateRecord("Sample Record Id1", "Sample Data1")
	record1.GetHeader().SetAttribute("Sample Attribute", "Sample Value1")

	record2, _ := st.CreateRecord("Sample Record Id2", complexRecordField)
	record2.GetHeader().SetAttribute("Sample Attribute", "Sample Value2")

	bufferWriter := bytes.NewBuffer([]byte{})

	recordWriterFactory := &SDCRecordWriterFactoryImpl{}

	recordWriter, err := recordWriterFactory.CreateWriter(st, bufferWriter)

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

	if err = recordWriter.Flush(); err != nil {
		t.Error(err)
	}
	if err = recordWriter.Close(); err != nil {
		t.Error(err)
	}
}

func checkField(t *testing.T, actual *api.Field, expected *api.Field) {
	if reflect.TypeOf(actual) != reflect.TypeOf(expected) {
		t.Fatalf("Type %s does not match %s", reflect.TypeOf(actual), reflect.TypeOf(expected))
	} else {
		switch actual.Type {
		case fieldtype.MAP:
			mapField1 := actual.Value.(map[string]*api.Field)
			mapField2 := expected.Value.(map[string]*api.Field)
			if len(mapField1) != len(mapField2) {
				t.Fatal("Map Length does not match")
			}
			for k, v1 := range mapField1 {
				v2, ok := mapField2[k]
				if !ok {
					t.Fatalf("Key %s does not exist in map", k)
				}
				checkField(t, v1, v2)
			}
		case fieldtype.LIST_MAP:
			listMapField1 := actual.Value.(*linkedhashmap.Map)
			listMapField2 := expected.Value.(*linkedhashmap.Map)
			if listMapField1.Size() != listMapField2.Size() {
				t.Fatal("List Map Length does not match")
			}

			it := listMapField1.Iterator()
			for it.HasNext() {
				entry := it.Next()
				key := entry.GetKey()
				v1 := entry.GetValue().(*api.Field)
				var v2 *api.Field
				if v, found := listMapField2.Get(key); !found {
					t.Fatalf("Key %s does not exist in map", key)
				} else {
					v2 = v.(*api.Field)
				}
				checkField(t, v1, v2)
			}
		case fieldtype.LIST:
			listField1 := actual.Value.([]*api.Field)
			listField2 := expected.Value.([]*api.Field)
			if len(listField1) != len(listField2) {
				t.Fatal("List Length does not match")
			}
			for i, v1 := range listField1 {
				v2 := listField2[i]
				checkField(t, v1, v2)
			}
		case fieldtype.STRING:
			stringVal1 := actual.Value.(string)
			stringVal2 := expected.Value.(string)
			if strings.Compare(stringVal1, stringVal2) != 0 {
				t.Fatalf("String %s does not match %s", stringVal1, stringVal2)
			}
		case fieldtype.BYTE_ARRAY:
			byteArray1 := actual.Value.([]byte)
			byteArray2 := expected.Value.([]byte)
			if bytes.Compare(byteArray1, byteArray2) != 0 {
				t.Fatalf(
					"Byte arrays does not match. Expected : %s Actual: %s",
					string(byteArray1),
					string(byteArray2),
				)
			}
		default:
			if actual.Value != expected.Value {
				t.Fatalf("Value %v does not match %v for type %s", actual.Value, expected.Value, actual.Type)
			}
		}
	}
}

func checkRecord(t *testing.T, r api.Record, sourceId string, expectedRootField *api.Field, headersToCheck map[string]string) {
	isError := false

	if r.GetHeader().GetSourceId() != sourceId {
		t.Errorf(
			"SourceId does not match, Expected :%s, Actual : %s",
			sourceId,
			r.GetHeader().GetSourceId(),
		)
		isError = true
	}

	actualRootField, err := r.Get()
	if err != nil {
		t.Fatal(err)
	}
	actualHeaders := r.GetHeader().GetAttributes()

	for k, v := range headersToCheck {
		if actualHeaders[k] != v {
			t.Errorf(
				"Header does not match for Record Id:%s and Attribute : %s, Expected :%s, Actual : %s",
				r.GetHeader().GetSourceId(),
				k,
				v,
				actualHeaders[k],
			)
		}
	}

	checkField(t, actualRootField, expectedRootField)
	if isError {
		t.Fatalf("Error happened when checking record : %s", r.GetHeader().GetSourceId())
	}
}

func TestReadRecord(t *testing.T) {
	st := CreateStageContext()

	recordReaderFactory := &SDCRecordReaderFactoryImpl{}

	reader, err := recordReaderFactory.CreateReader(st, strings.NewReader(JSON), "m")

	if err != nil {
		t.Fatal(err.Error())
	}

	recordCounter := 0

	end := false
	for !end {
		r, err := reader.ReadRecord()
		if err != nil {
			t.Fatal(err.Error())
		}

		if r == nil {
			end = true
		} else {
			if recordCounter > 1 {
				t.Fatal("Only Two Records were defined in the reader, but reader is reading more than that")
			}
			if recordCounter == 0 {
				f, err := api.CreateField("Sample Data1")
				if err != nil {
					t.Fatal(err)
				}
				checkRecord(
					t,
					r,
					"Sample Record Id1",
					f,
					map[string]string{"Sample Attribute": "Sample Value1"},
				)
			} else {
				f, err := api.CreateField(complexRecordField)
				if err != nil {
					t.Fatal(err)
				}
				checkRecord(
					t,
					r,
					"Sample Record Id2",
					f,
					map[string]string{"Sample Attribute": "Sample Value2"},
				)
			}
		}
		recordCounter++
	}
}

func TestReadAndWriteRecord(t *testing.T) {
	st := CreateStageContext()
	expectedRecords := make([]api.Record, 0)

	record1, err := st.CreateRecord("Sample Record Id1", "Sample Data1")
	if err != nil {
		t.Fatal(err)
	}
	record1.GetHeader().SetAttribute("Sample Attribute", "Sample Value1")
	expectedRecords = append(expectedRecords, record1)

	record2, err := st.CreateRecord("Sample Record Id2", complexRecordField)
	if err != nil {
		t.Fatal(err)
	}
	record2.GetHeader().SetAttribute("Sample Attribute", "Sample Value2")
	expectedRecords = append(expectedRecords, record2)

	record3, err := st.CreateRecord("Sample Record Id3", getAllTypesRecordField())
	if err != nil {
		t.Fatal(err)
	}
	record3.GetHeader().SetAttribute("Sample Attribute", "Sample Value3")
	expectedRecords = append(expectedRecords, record3)

	bufferWriter := bytes.NewBuffer([]byte{})

	recordWriterFactory := &SDCRecordWriterFactoryImpl{}

	recordWriter, err := recordWriterFactory.CreateWriter(st, bufferWriter)

	if err != nil {
		t.Fatal(err)
	}

	for _, r := range expectedRecords {
		err = recordWriter.WriteRecord(r)
		if err != nil {
			t.Fatal(err)
		}
	}

	recordWriter.Flush()
	recordWriter.Close()

	recordReaderFactory := &SDCRecordReaderFactoryImpl{}

	reader, err := recordReaderFactory.CreateReader(st, bytes.NewReader(bufferWriter.Bytes()), "m")

	if err != nil {
		t.Fatal(err.Error())
	}

	actualRecords := make([]api.Record, 0)

	end := false
	for !end {
		r, err := reader.ReadRecord()
		if err != nil {
			t.Fatal(err.Error())
		}

		if r == nil {
			end = true
		} else {
			actualRecords = append(actualRecords, r)
		}
	}

	if len(actualRecords) != len(expectedRecords) {
		t.Fatalf(
			"Number of records wrote and read does not match, Written : %d, Read: %d",
			len(expectedRecords),
			len(actualRecords),
		)
	}

	for i := 0; i < len(expectedRecords); i++ {
		expectedRecord := expectedRecords[i]
		if rootField, err := expectedRecord.Get(); err == nil {
			checkRecord(
				t,
				actualRecords[i],
				expectedRecord.GetHeader().GetSourceId(),
				rootField,
				expectedRecord.GetHeader().GetAttributes(),
			)
		} else {
			t.Fatal(err)
		}
	}
}

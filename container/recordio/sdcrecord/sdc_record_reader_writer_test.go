package sdcrecord

import (
	"bytes"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"strings"
	"testing"
)

const (
	JSON1 = "{\"header\":{\"stageCreator\":\"Dummy Stage\",\"sourceId\":\"Sample Record Id1\"," +
		"\"stagesPath\":\"\",\"trackingId\":\"\",\"previousTrackingId\":\"\"," +
		"\"errorDataCollectorId\":\"\",\"errorPipelineName\":\"\",\"errorStage\":\"\"," +
		"\"errorMessage\":\"\",\"errorTimestamp\":0,\"values\":{\"Sample Attribute\":\"Sample Value1\"}}," +
		"\"value\":{\"type\":\"STRING\",\"value\":\"Sample Data1\",\"sqpath\":\"/\",\"dqpath\":\"/\"}}\n"
	JSON2 = "{\"header\":{\"stageCreator\":\"Dummy Stage\",\"sourceId\":\"Sample Record Id2\"," +
		"\"stagesPath\":\"\",\"trackingId\":\"\",\"previousTrackingId\":\"\"," +
		"\"errorDataCollectorId\":\"\",\"errorPipelineName\":\"\",\"errorStage\":\"\"," +
		"\"errorMessage\":\"\",\"errorTimestamp\":0,\"values\":{\"Sample Attribute\":\"Sample Value2\"}}," +
		"\"value\":{\"type\":\"STRING\",\"value\":\"Sample Data2\",\"sqpath\":\"/\",\"dqpath\":\"/\"}}\n"
)

var JSON = string([]byte{JSON1_MAGIC_NUMBER}) + JSON1 + JSON2

func CreateStageContext() api.StageContext {
	return &common.StageContextImpl{
		StageConfig: common.StageConfiguration{InstanceName: "Dummy Stage"},
		Parameters:  nil,
	}
}

func TestWriteRecord(t *testing.T) {
	st := CreateStageContext()
	record1 := st.CreateRecord("Sample Record Id1", "Sample Data1")
	record1.GetHeader().SetAttribute("Sample Attribute", "Sample Value1")

	record2 := st.CreateRecord("Sample Record Id2", "Sample Data2")
	record2.GetHeader().SetAttribute("Sample Attribute", "Sample Value2")

	bufferWriter := bytes.NewBuffer([]byte{})

	recordWriterFactory := &SDCRecordWriterFactoryImpl{}

	record_writer, err := recordWriterFactory.CreateWriter(st, bufferWriter)

	if err != nil {
		t.Fatal(err)
	}

	err = record_writer.WriteRecord(record1)
	if err != nil {
		t.Fatal(err)
	}

	err = record_writer.WriteRecord(record2)
	if err != nil {
		t.Fatal(err)
	}

	record_writer.Flush()
	record_writer.Close()

	if bytes.Compare([]byte(JSON), bufferWriter.Bytes()) != 0 {
		t.Fatalf(
			"Serialization Wrong. Expected : %s Actual: %s",
			JSON,
			bufferWriter.String(),
		)
	}
}

func checkRecord(t *testing.T, r api.Record, sourceId string, value interface{}, headersToCheck map[string]string) {
	isError := false

	if r.GetHeader().GetSourceId() != sourceId {
		t.Errorf(
			"SourceId does not match, Expected :%s, Actual : %s",
			sourceId,
			r.GetHeader().GetSourceId(),
		)
		isError = true
	}

	if r.GetValue() != value {
		t.Errorf(
			"Value does not match for Record Id:%s, Expected :%s, Actual : %s",
			r.GetHeader().GetSourceId(),
			value, r.GetValue(),
		)
		isError = true
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
			isError = true
		}
	}
	if isError {
		t.Fatalf("Error happened when checking record : %s", r.GetHeader().GetSourceId())
	}
}

func TestReadRecord(t *testing.T) {
	st := CreateStageContext()

	recordReaderFactory := &SDCRecordReaderFactoryImpl{}

	reader, err := recordReaderFactory.CreateReader(st, strings.NewReader(JSON))

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
				checkRecord(
					t,
					r,
					"Sample Record Id1",
					"Sample Data1",
					map[string]string{"Sample Attribute": "Sample Value1"},
				)
			} else {
				checkRecord(
					t,
					r,
					"Sample Record Id2",
					"Sample Data2",
					map[string]string{"Sample Attribute": "Sample Value2"},
				)
			}
		}
		recordCounter++
	}
}

func TestReadAndWriteRecord(t *testing.T) {
	st := CreateStageContext()
	expectedRecords := []api.Record{}

	record1 := st.CreateRecord("Sample Record Id1", "Sample Data1")
	record1.GetHeader().SetAttribute("Sample Attribute", "Sample Value1")
	expectedRecords = append(expectedRecords, record1)

	record2 := st.CreateRecord("Sample Record Id2", "Sample Data2")
	record2.GetHeader().SetAttribute("Sample Attribute", "Sample Value2")
	expectedRecords = append(expectedRecords, record2)

	record3 := st.CreateRecord("Sample Record Id3", "Sample Data3")
	record3.GetHeader().SetAttribute("Sample Attribute", "Sample Value3")
	expectedRecords = append(expectedRecords, record3)

	bufferWriter := bytes.NewBuffer([]byte{})

	recordWriterFactory := &SDCRecordWriterFactoryImpl{}

	record_writer, err := recordWriterFactory.CreateWriter(st, bufferWriter)

	if err != nil {
		t.Fatal(err)
	}

	for _, r := range expectedRecords {
		err = record_writer.WriteRecord(r)
		if err != nil {
			t.Fatal(err)
		}
	}

	record_writer.Flush()
	record_writer.Close()

	recordReaderFactory := &SDCRecordReaderFactoryImpl{}

	reader, err := recordReaderFactory.CreateReader(st, bytes.NewReader(bufferWriter.Bytes()))

	if err != nil {
		t.Fatal(err.Error())
	}

	actualRecords := []api.Record{}

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
		checkRecord(
			t,
			actualRecords[i],
			expectedRecord.GetHeader().GetSourceId(),
			expectedRecord.GetValue(),
			expectedRecord.GetHeader().GetAttributes(),
		)
	}
}

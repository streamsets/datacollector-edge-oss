package jsonrecord

import (
	"bytes"
	"encoding/json"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/fieldtype"
	"github.com/streamsets/datacollector-edge/container/common"
	"testing"
)

func CreateStageContext() api.StageContext {
	return &common.StageContextImpl{
		StageConfig: common.StageConfiguration{InstanceName: "Dummy Stage"},
		Parameters:  nil,
	}
}

func TestReadMapRecord(t *testing.T) {
	commits := map[string]interface{}{
		"rsc": "rscValue",
		"r":   "rValue",
		"gri": "1908",
		"adg": "912",
	}
	bufferWriter := bytes.NewBuffer([]byte{})
	encoder := json.NewEncoder(bufferWriter)
	encoder.Encode(commits)

	stageContext := CreateStageContext()
	readerFactoryImpl := &JsonReaderFactoryImpl{}
	recordReader, err := readerFactoryImpl.CreateReader(stageContext, bufferWriter)
	if err != nil {
		t.Fatal(err.Error())
	}

	record, err := recordReader.ReadRecord()
	if err != nil {
		t.Fatal(err.Error())
	}

	rootField, _ := record.Get()
	if rootField.Type != fieldtype.MAP {
		t.Errorf("Excpeted record type : Map, but received: %s", rootField.Type)
	}

	mapField := rootField.Value.(map[string]*api.Field)
	for fieldName, fieldValue := range mapField {
		if commits[fieldName] != fieldValue.Value {
			t.Errorf("Excpeted field value : %s, but received: %s", commits[fieldName], fieldValue.Value)
		}
	}
}

func TestWriteAndReadStringRecord(t *testing.T) {
	stageContext := CreateStageContext()
	record1, _ := stageContext.CreateRecord("Id1", "Sample Data1")
	record1.GetHeader().SetAttribute("Sample Attribute", "Sample Value1")

	record2, _ := stageContext.CreateRecord("Id2", "Sample Data2")
	record2.GetHeader().SetAttribute("Sample Attribute", "Sample Value2")

	bufferWriter := bytes.NewBuffer([]byte{})

	recordWriterFactory := &JsonWriterFactoryImpl{}
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

	recordWriter.Flush()
	recordWriter.Close()

	readerFactoryImpl := &JsonReaderFactoryImpl{}
	recordReader, err := readerFactoryImpl.CreateReader(stageContext, bufferWriter)
	if err != nil {
		t.Fatal(err.Error())
	}

	recordCounter := 0

	end := false
	for !end {
		r, err := recordReader.ReadRecord()
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
				rootField, _ := r.Get()
				if rootField.Value != "Sample Data1" {
					t.Errorf("Excepted: Sample Data1, but got: %s", rootField.Value)
				}
			} else {
				rootField, _ := r.Get()
				if rootField.Value != "Sample Data2" {
					t.Errorf("Excepted: Sample Data2, but got: %s", rootField.Value)
				}
			}
		}
		recordCounter++
	}

	recordReader.Close()
}

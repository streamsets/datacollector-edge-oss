package jsonrecord

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestWriteMapRecord(t *testing.T) {
	stageContext := CreateStageContext()
	commits := map[string]interface{}{
		"rsc": 3711,
		"r":   2138,
		"gri": 1908,
		"adg": 912,
	}
	record1, err := stageContext.CreateRecord("Id1", commits)
	if err != nil {
		t.Fatal(err)
	}
	record1.GetHeader().SetAttribute("Sample Attribute", "Sample Value1")

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

	recordWriter.Flush()
	recordWriter.Close()

	decoder := json.NewDecoder(bufferWriter)
	var recordObject = make(map[string]int)
	err = decoder.Decode(&recordObject)
	if err != nil {
		t.Fatal(err)
	}

	if recordObject["rsc"] != commits["rsc"] {
		t.Errorf("Excepted: %s, but got: %s", commits["rsc"], recordObject["rsc"])
	}

	if recordObject["r"] != commits["r"] {
		t.Errorf("Excepted: %s, but got: %s", commits["r"], recordObject["r"])
	}

	if recordObject["gri"] != commits["gri"] {
		t.Errorf("Excepted: %s, but got: %s", commits["gri"], recordObject["gri"])
	}

	if recordObject["adg"] != commits["adg"] {
		t.Errorf("Excepted: %s, but got: %s", commits["adg"], recordObject["adg"])
	}
}

func TestWriteListRecord(t *testing.T) {
	stageContext := CreateStageContext()
	stringSlice := []interface{}{"apple", "orange", "banana"}
	record1, err := stageContext.CreateRecord("Id1", stringSlice)
	if err != nil {
		t.Fatal(err)
	}
	record1.GetHeader().SetAttribute("Sample Attribute", "Sample Value1")
	bufferWriter := bytes.NewBuffer([]byte{})

	//
	recordWriterFactory := &JsonWriterFactoryImpl{}
	recordWriter, err := recordWriterFactory.CreateWriter(stageContext, bufferWriter)
	if err != nil {
		t.Fatal(err)
	}
	err = recordWriter.WriteRecord(record1)
	if err != nil {
		t.Fatal(err)
	}
	recordWriter.Flush()
	recordWriter.Close()

	decoder := json.NewDecoder(bufferWriter)
	var listRecordObject = []interface{}{}
	err = decoder.Decode(&listRecordObject)
	if err != nil {
		t.Fatal(err)
	}

	if len(listRecordObject) != 3 {
		t.Errorf("Excepted length: 3, but got: %d", len(listRecordObject))
	}

	if listRecordObject[0] != stringSlice[0] {
		t.Errorf("Excepted: %s, but got: %s", stringSlice[0], listRecordObject[0])
	}
}

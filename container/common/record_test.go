package common

import (
	"github.com/streamsets/datacollector-edge/api"
	"testing"
)

func TestRecordImpl_GetFromPath(t *testing.T) {
	rootRecord := make(map[string]interface{})

	mapOfMaps := make(map[string]interface{})
	innerMap := map[string]interface{}{
		"innerMapValue": "innerMapValue",
	}
	mapOfMaps["innerMap"] = innerMap
	rootRecord["mapOfMaps"] = mapOfMaps

	rootRecord["listField"] = []string{"a", "b"}

	rootRecord["listOfMapsField"] = []interface{}{
		map[string]interface{}{"a": "a1", "b": "b1"},
		map[string]interface{}{"a": "a2", "b": "b2"},
	}

	record, err := createRecord("recordSourceId", rootRecord)
	if err != nil {
		t.Error(record)
	}

	//mapOfMapsField
	mapOfMapsInnerMapVal, err := record.Get("/mapOfMaps/innerMap/innerMapValue")
	if err != nil {
		t.Error(record)
	}

	if mapOfMapsInnerMapVal.Value != nil && mapOfMapsInnerMapVal.Value != "innerMapValue" {
		t.Errorf("Expected value 'innerMapValue', but got %s", mapOfMapsInnerMapVal.Value)
	}

	//listField
	listValue0, err := record.Get("/listField[0]")
	if err != nil {
		t.Errorf("Error accessing /listField[0], Error : %s", err.Error())
	}

	if listValue0.Value != "a" {
		t.Errorf("Expected : 'a', Found : %v", listValue0.Value)
	}

	listValue1, err := record.Get("/listField[1]")
	if err != nil {
		t.Errorf("Error accessing /listField[1], Error : %s", err.Error())
	}

	if listValue1.Value != "b" {
		t.Errorf("Expected : 'b', Found : %v", listValue0.Value)
	}

	//listOfMapsField
	listOfMaps0a, err := record.Get("/listOfMapsField[0]/a")

	if err != nil {
		t.Errorf("Error accessing listOfMapsField[0]/a, Error : %s", err.Error())
	}

	if listOfMaps0a.Value != "a1" {
		t.Errorf("Expected : 'a1', Found : %v", listOfMaps0a.Value)
	}

	listOfMaps0b, err := record.Get("/listOfMapsField[0]/b")

	if err != nil {
		t.Errorf("Error accessing listOfMapsField[0]/b, Error : %s", err.Error())
	}

	if listOfMaps0b.Value != "b1" {
		t.Errorf("Expected : 'b1', Found : %v", listOfMaps0b.Value)
	}

	listOfMaps1a, err := record.Get("/listOfMapsField[1]/a")

	if err != nil {
		t.Errorf("Error accessing listOfMapsField[1]/a, Error : %s", err.Error())
	}

	if listOfMaps1a.Value != "a2" {
		t.Errorf("Expected : 'a2', Found : %v", listOfMaps1a.Value)
	}

	listOfMaps1b, err := record.Get("/listOfMapsField[1]/b")

	if err != nil {
		t.Errorf("Error accessing listOfMapsField[1]/b, Error : %s", err.Error())
	}

	if listOfMaps1b.Value != "b2" {
		t.Errorf("Expected : 'b2', Found : %v", listOfMaps1b.Value)
	}
}

func checkFieldCloned(t *testing.T, fieldPath string, realRecord api.Record, clonedRecord api.Record) {
	realFieldPtr, rerr := realRecord.Get(fieldPath)
	clonedFieldPtr, crerr := clonedRecord.Get(fieldPath)

	if rerr != nil {
		t.Errorf("Error Getting Field '{}' from Real Record. Reason : '%s' ", rerr.Error())
	}

	if crerr != nil {
		t.Errorf("Error Getting Field '{}' from Cloned Record. Reason : '%s' ", crerr.Error())
	}

	if realFieldPtr == clonedFieldPtr {
		t.Errorf("Field '%s' has the same address '%p'", fieldPath, realFieldPtr)
	}

	realFieldValue := realFieldPtr.Value
	clonedFieldValue := clonedFieldPtr.Value

	if (&realFieldValue) == (&clonedFieldValue) {
		t.Errorf("Field Value '%s' has the same address '%p' ", fieldPath, (&realFieldValue))
	}
}

func TestRecordImpl_Clone(t *testing.T) {
	rootField := make(map[string]interface{})
	stringField := "stringField"
	intField := int64(1)
	floatField := float64(1.01)
	mapField := map[string]interface{}{"a": 1, "b": 2}
	stringListField := []string{"a", "b"}
	listField := []interface{}{1, 2}

	rootField["stringField"] = stringField
	rootField["intField"] = intField
	rootField["floatField"] = floatField
	rootField["mapField"] = mapField
	rootField["stringListField"] = stringListField
	rootField["listField"] = listField

	record, err := createRecord("recordSourceId", rootField)
	if err != nil {
		t.Error(record)
	}
	record.GetHeader().SetAttribute("a", "1")
	record.GetHeader().SetAttribute("b", "2")

	clonedRecordPtr := record.Clone().(*RecordImpl)
	realRecordPtr := record.(*RecordImpl)

	if clonedRecordPtr == realRecordPtr {
		t.Error("Record is not cloned")
	}

	checkFieldCloned(t, "/stringField", realRecordPtr, clonedRecordPtr)
	checkFieldCloned(t, "/intField", realRecordPtr, clonedRecordPtr)
	checkFieldCloned(t, "/floatField", realRecordPtr, clonedRecordPtr)
	checkFieldCloned(t, "/mapField", realRecordPtr, clonedRecordPtr)
	checkFieldCloned(t, "/mapField/a", realRecordPtr, clonedRecordPtr)
	checkFieldCloned(t, "/mapField/b", realRecordPtr, clonedRecordPtr)
	checkFieldCloned(t, "/stringListField", realRecordPtr, clonedRecordPtr)
	checkFieldCloned(t, "/stringListField[0]", realRecordPtr, clonedRecordPtr)
	checkFieldCloned(t, "/stringListField[1]", realRecordPtr, clonedRecordPtr)
	checkFieldCloned(t, "/listField", realRecordPtr, clonedRecordPtr)
	checkFieldCloned(t, "/listField[0]", realRecordPtr, clonedRecordPtr)
	checkFieldCloned(t, "/listField[1]", realRecordPtr, clonedRecordPtr)
}

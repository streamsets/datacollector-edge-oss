package common

import (
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

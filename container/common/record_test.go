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
package common

import (
	"fmt"
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

func TestRecordImpl_Delete(t *testing.T) {
	rootField := make(map[string]interface{})
	stringField := "stringField"
	mapField := map[string]interface{}{"primitive": 1, "map": map[string]interface{}{"a": 1, "b": 2}}
	listField := []interface{}{1, 2}

	rootField["primitiveField"] = stringField
	rootField["mapField"] = mapField
	rootField["listField"] = listField

	record, err := createRecord("recordSourceId", rootField)
	if err != nil {
		t.Error(record)
	}

	//map
	mapDeletedField, err := record.Delete("/mapField/map/a")

	if err != nil || mapDeletedField.Value.(int) != 1 {
		t.Error("/mapField/map/a is not deleted, value does not match")
	}

	//list
	list1Field, err := record.Delete("/listField[0]")

	if err != nil || list1Field.Value.(int) != 1 {
		t.Error("/listField[0] is not deleted, value does not match")
	}

	list1Field, err = record.Get("/listField[0]")

	if err != nil || list1Field.Value.(int) != 2 {
		t.Error("/listField[0] is not correct after deletion, value does not match")
	}

	//rootMap
	root, err := record.Delete("/")

	if err != nil || root == nil {
		t.Error("Error when removing root map field")
	}

	if f, _ := record.Get("/"); f.Value != nil {
		t.Error("Root map not deleted")
	}
}

func TestRecordImpl_Set(t *testing.T) {
	rootField := make(map[string]interface{})
	mapField := map[string]interface{}{"primitive": 1, "map": map[string]interface{}{"a": 1, "b": 2}}
	listField := []string{"a", "b"}

	rootField["mapField"] = mapField
	rootField["listField"] = listField

	record, err := createRecord("recordSourceId", rootField)
	if err != nil {
		t.Error(record)
	}

	//To be set field
	f, err := api.CreateField("newField")
	if err != nil {
		t.Error(record)
	}

	//non existing parent map field
	_, err = record.SetField("/a/b", f)

	if err == nil {
		t.Error("Should error for non existing field /a/b")
	}

	//non existing map field
	exF, err := record.SetField("/a", f)
	if err != nil || exF != nil {
		t.Error("Error setting field /a")
	}

	getF, err := record.Get("/a")

	if err != nil || getF.Value.(string) != "newField" {
		t.Error("Error getting set field /a")
	}

	//List index greater than list size
	_, err = record.SetField("/listField[3]", f)
	if err == nil {
		t.Error("Should error for non existing field /listfield[2]")
	}

	//Append to a list
	exF, err = record.SetField("/listField[2]", f)
	if err != nil || exF != nil {
		t.Error("Error setting field /listField[2]")
	}
	getF, err = record.Get("/listField[2]")
	if err != nil || getF.Value.(string) != "newField" {
		t.Error("Error getting set field /a")
	}

	//Insert in the middle of a list getting the existing field
	exF, err = record.SetField("/listField[1]", f)
	if err != nil || exF.Value.(string) != "b" {
		t.Error("Error setting field /listField[2] or the api did not return the correect existing value")
	}

	getF, err = record.Get("/listField[1]")
	if err != nil || getF.Value.(string) != "newField" {
		t.Error("Error getting set field /a")
	}

	//Setting an inner map field
	exF, err = record.SetField("/mapField/c", f)
	if err != nil || exF != nil {
		t.Error("Error setting field /mapField/c")
	}
	getF, err = record.Get("/mapField/c")
	if err != nil || getF.Value.(string) != "newField" {
		t.Error("Error getting set field /mapField/c")
	}
}

func TestRecordImpl_GetFieldPaths(t *testing.T) {
	rootField := make(map[string]interface{})
	stringField := "stringField"
	mapField := map[string]interface{}{"primitive": 1, "map": map[string]interface{}{"a": 1, "b": 2}}
	listField := []interface{}{1, 2}

	rootField["primitiveField"] = stringField
	rootField["mapField"] = mapField
	rootField["listField"] = listField

	record, err := createRecord("recordSourceId", rootField)
	if err != nil {
		t.Fatal(record)
	}

	expectedFieldPaths := []string{
		"/primitiveField",
		"/mapField",
		"/mapField/primitive",
		"/mapField/map",
		"/mapField/map/a",
		"/mapField/map/b",
		"/listField",
		"/listField[0]",
		"/listField[1]",
	}

	actualFieldPaths := record.GetFieldPaths()

	for _, fieldPath := range expectedFieldPaths {
		_, exists := actualFieldPaths[fieldPath]
		if !exists {
			t.Error(fmt.Sprintf("Field Path '%s' is expected but not returned", fieldPath))
		}
	}
}

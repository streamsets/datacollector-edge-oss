package common

import "testing"

func TestRecordImpl_GetFromPath(t *testing.T) {
	rootRecord := make(map[string]interface{})
	fieldA := make(map[string]interface{})
	fieldB := map[string]interface{}{
		"c": "CValue",
	}
	fieldA["b"] = fieldB
	rootRecord["a"] = fieldA

	record, err := createRecord("recordSourceId", rootRecord)
	if err != nil {
		t.Error(record)
	}

	cValue, err := record.Get("/a/b/c")
	if err != nil {
		t.Error(record)
	}

	if cValue.Value != nil && cValue.Value != "CValue" {
		t.Errorf("Expected value 'CValue', but got %s", cValue.Value)
	}
}

package common

import (
	"fmt"
	"testing"
)

func TestCreateMapElement(t *testing.T) {
	pathElement := CreateMapElement("hello")
	if pathElement.Name != "hello" {
		t.Errorf("Excepted hello, but got %s", pathElement.Name)
	}

	if pathElement.Type != MAP {
		t.Errorf("Excepted MAP, but got %s", pathElement.Type)
	}

	if pathElement.Idx != 0 {
		t.Errorf("Excepted 0, but got %d", pathElement.Idx)
	}
}

func TestCreateListElement(t *testing.T) {
	pathElement := CreateListElement(5)
	if pathElement.Name != "" {
		t.Errorf("Excepted empty, but got %s", pathElement.Name)
	}

	if pathElement.Type != LIST {
		t.Errorf("Excepted LIST, but got %s", pathElement.Type)
	}

	if pathElement.Idx != 5 {
		t.Errorf("Excepted 5, but got %d", pathElement.Idx)
	}
}

func TestParseMap(t *testing.T) {
	pathElementList, err := ParseFieldPath("/a/b/c", false)

	if err != nil {
		t.Error(err)
	}
	if pathElementList == nil {
		t.Error("Parsing field path is failed")
	}

	if len(pathElementList) < 4 {
		t.Error("Failed to parse field path")
		return
	}

	if pathElementList[0].Type != ROOT {
		t.Errorf("Excepted Root, but got %s", pathElementList[0].Type)
	}

	if pathElementList[1].Name != "a" {
		t.Errorf("Excepted a, but got %s", pathElementList[1].Name)
	}

	if pathElementList[2].Name != "b" {
		t.Errorf("Excepted b, but got %s", pathElementList[2].Name)
	}

	if pathElementList[3].Name != "c" {
		t.Errorf("Excepted c, but got %s", pathElementList[3].Name)
	}

	for _, pathElement := range pathElementList {
		fmt.Println(pathElement)
	}
}

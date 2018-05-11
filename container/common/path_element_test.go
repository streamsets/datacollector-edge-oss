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

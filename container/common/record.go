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
	"errors"
	"fmt"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/fieldtype"
	"github.com/streamsets/datacollector-edge/api/linkedhashmap"
	"strings"
)

type RecordImpl struct {
	header *HeaderImpl
	value  *api.Field
}

func (r *RecordImpl) GetHeader() api.Header {
	return r.header
}

func (r *RecordImpl) Get(fieldPath ...string) (*api.Field, error) {
	if len(fieldPath) == 0 {
		return r.value, nil
	} else {
		field := &api.Field{}
		pathElements, err := r.parse(fieldPath[0])
		if err != nil {
			return field, err
		}
		fields := r.getFromPathElements(pathElements)
		if len(pathElements) == len(fields) {
			return fields[len(fields)-1], nil
		} else {
			return field, nil
		}
	}
}

func (r *RecordImpl) GetFieldPaths() map[string]bool {
	return r.gatherPaths("/", r.value) // TODO:SDCE-128 - Implement escaping in GetFieldPaths
}

func (r *RecordImpl) gatherPaths(prefix string, currentField *api.Field) map[string]bool {
	gatheredPaths := map[string]bool{}
	if strings.HasSuffix(prefix, "/") {
		prefix = strings.TrimRight(prefix, "/")
	}
	gatheredPaths[prefix] = true
	switch currentField.Type {
	case fieldtype.LIST:
		listField := currentField.Value.([]*api.Field)
		gatheredPaths[prefix] = true
		for idx, idxField := range listField {
			childGatheredPaths := r.gatherPaths(fmt.Sprintf(prefix+"[%d]", idx), idxField)
			for k, v := range childGatheredPaths {
				gatheredPaths[k] = v
			}
		}
	case fieldtype.MAP:
		mapField := currentField.Value.(map[string]*api.Field)
		for fieldKey, fieldValue := range mapField {
			childGatheredPaths := r.gatherPaths(fmt.Sprintf(prefix+"/%s", fieldKey), fieldValue)
			for k, v := range childGatheredPaths {
				gatheredPaths[k] = v
			}
		}
	case fieldtype.LIST_MAP:
		listMapValue := currentField.Value.(*linkedhashmap.Map)
		it := listMapValue.Iterator()
		for it.HasNext() {
			entry := it.Next()
			fieldKey := entry.GetKey()
			fieldValue := entry.GetValue().(*api.Field)
			childGatheredPaths := r.gatherPaths(fmt.Sprintf(prefix+"/%s", fieldKey), fieldValue)
			for k, v := range childGatheredPaths {
				gatheredPaths[k] = v
			}
		}
	}
	return gatheredPaths
}

func (r *RecordImpl) Clone() api.Record {
	recordVal, _ := r.Get()
	if recordVal != nil {
		recordVal = recordVal.Clone()
	}
	return &RecordImpl{header: ((r.GetHeader()).(*HeaderImpl)).clone(), value: recordVal}
}

func (r *RecordImpl) parse(fieldPath string) ([]PathElement, error) {
	return ParseFieldPath(fieldPath, true)
}

func (r *RecordImpl) getFromPathElements(pathElements []PathElement) []*api.Field {
	fields := make([]*api.Field, 0)
	if r.value != nil {
		current := r.value
		for _, pathElement := range pathElements {
			if current == nil {
				break
			}
			var next *api.Field
			switch pathElement.Type {
			case ROOT:
				fields = append(fields, current)
				next = current
			case MAP:
				if current.Type == fieldtype.MAP {
					mapValue := current.Value.(map[string](*api.Field))
					if mapValue != nil {
						field, ok := mapValue[pathElement.Name]
						if !ok {
							return fields
						} else if len(field.Type) > 0 {
							fields = append(fields, field)
							next = field
						}
					}
				} else if current.Type == fieldtype.LIST_MAP {
					if current.Value != nil {
						listMapValue := current.Value.(*linkedhashmap.Map)
						if f, ok := listMapValue.Get(pathElement.Name); !ok {
							return fields
						} else {
							field := f.(*api.Field)
							if len(field.Type) > 0 {
								fields = append(fields, field)
								next = field
							}
						}
					}
				}
			case LIST:
				if current.Type == fieldtype.LIST {
					listValue := current.Value.([]*api.Field)
					if listValue != nil && len(listValue) > pathElement.Idx {
						field := listValue[pathElement.Idx]
						if len(field.Type) > 0 {
							fields = append(fields, field)
							next = field
						}
					}
				}
			}
			current = next
		}
	}
	return fields
}

func (r *RecordImpl) Set(field *api.Field) *api.Field {
	oldData := r.value
	r.value = field
	return oldData
}

func (r *RecordImpl) SetField(fieldPath string, field *api.Field) (*api.Field, error) {
	pathElements, err := r.parse(fieldPath)
	var fieldToReplace *api.Field = nil
	if err == nil {
		fields := r.getFromPathElements(pathElements)
		fieldPos := len(fields)
		if len(pathElements) == fieldPos {
			fieldPos--
			fieldToReplace, err = r.doSet(fieldPos, field, pathElements, fields)
		} else if len(pathElements)-1 == fieldPos {
			fieldToReplace, err = r.doSet(fieldPos, field, pathElements, fields)
		} else {
			err = errors.New("Field-path " + fieldPath + " not reachable")
		}
	}
	return fieldToReplace, err
}

func (r *RecordImpl) doSet(fieldPos int, newField *api.Field, pathElements []PathElement, fields []*api.Field) (*api.Field, error) {
	var fieldToReplace *api.Field = nil
	var err error = nil
	if fieldPos == 0 {
		fieldToReplace = r.value
		r.value = newField
	} else {
		elem := pathElements[fieldPos]
		switch elem.Type {
		case MAP:
			field := fields[fieldPos-1]
			if field.Value != nil {
				if field.Type == fieldtype.MAP {
					parent := field.Value.(map[string]*api.Field)
					fieldToReplace, _ = parent[elem.Name]
					parent[elem.Name] = newField
				} else if field.Type == fieldtype.LIST_MAP {
					parent := field.Value.(*linkedhashmap.Map)
					if listMapField, ok := parent.Get(elem.Name); ok {
						fieldToReplace = listMapField.(*api.Field)
					}
					parent.Put(elem.Name, newField)
				}
			}
		case LIST:
			parent := fields[fieldPos-1].Value.([]*api.Field)
			if elem.Idx > len(parent) {
				err = errors.New(fmt.Sprintf("Field Path index '%d' greater than current list element size '%d'",
					elem.Idx, len(parent)))
			} else if elem.Idx == len(parent) {
				//Reassign it to the underlying field slice
				fields[fieldPos-1].Value = append(parent, newField)
			} else {
				fieldToReplace = parent[elem.Idx]
				parent[elem.Idx] = newField
			}
		}
	}
	return fieldToReplace, err
}

func (r *RecordImpl) Delete(fieldPath string) (*api.Field, error) {
	pathElements, err := r.parse(fieldPath)
	if err != nil {
		return nil, err
	}
	fields := r.getFromPathElements(pathElements)

	var deletedField *api.Field = nil
	fieldPos := len(fields)
	if len(fields) == len(pathElements) {
		fieldPos--
		if fieldPos == 0 {
			deletedField = r.value
			r.value = nil
			return deletedField, nil
		} else {
			pathElement := pathElements[fieldPos]
			switch pathElement.Type {
			case MAP:
				parentField := fields[fieldPos-1].Value.(map[string](*api.Field))
				deletedField = fields[fieldPos]
				delete(parentField, pathElement.Name)
			case LIST:
				parentField := fields[fieldPos-1].Value.([]*api.Field)
				deleteIdx := pathElement.Idx
				deletedField = fields[fieldPos]
				fields[fieldPos-1].Value = append(parentField[:deleteIdx], parentField[deleteIdx+1:]...)
			default:
				return deletedField, errors.New("Unexpected field type " + pathElement.Name)
			}
		}
	}
	return deletedField, nil
}

type HeaderImpl struct {
	StageCreator         string                 `json:"stageCreator"`
	SourceId             string                 `json:"sourceId"`
	StagesPath           string                 `json:"stagesPath"`
	TrackingId           string                 `json:"trackingId"`
	PreviousTrackingId   string                 `json:"previousTrackingId"`
	ErrorDataCollectorId string                 `json:"errorDataCollectorId"`
	ErrorPipelineName    string                 `json:"errorPipelineName"`
	ErrorStageInstance   string                 `json:"errorStage"`
	ErrorMessage         string                 `json:"errorMessage"`
	ErrorTimestamp       int64                  `json:"errorTimestamp"`
	Attributes           map[string]interface{} `json:"values"`
	sourceRecord         api.Record
}

func (h *HeaderImpl) GetStageCreator() string {
	return h.StageCreator
}

func (h *HeaderImpl) GetSourceId() string {
	return h.SourceId
}

func (h *HeaderImpl) GetTrackingId() string {
	return h.TrackingId
}

func (h *HeaderImpl) GetPreviousTrackingId() string {
	return h.PreviousTrackingId
}

func (h *HeaderImpl) GetStagesPath() string {
	return h.StagesPath
}

func (h *HeaderImpl) GetErrorDataCollectorId() string {
	return h.ErrorDataCollectorId
}

func (h *HeaderImpl) GetErrorPipelineName() string {
	return h.ErrorPipelineName
}

func (h *HeaderImpl) GetErrorMessage() string {
	return h.ErrorMessage
}

func (h *HeaderImpl) GetErrorStage() string {
	return h.ErrorStageInstance
}

func (h *HeaderImpl) GetErrorTimestamp() int64 {
	return h.ErrorTimestamp
}

func (h *HeaderImpl) GetSourceRecord() api.Record {
	return h.sourceRecord
}

func (h *HeaderImpl) GetAttributeNames() []string {
	var attributeNames []string
	for k := range h.Attributes {
		attributeNames = append(attributeNames, k)
	}
	return attributeNames
}

func (h *HeaderImpl) GetAttributes() map[string]string {
	//Copy map
	attributes := make(map[string]string)
	for k := range h.Attributes {
		attributes[k] = h.Attributes[k].(string)
	}
	return attributes
}

func (h *HeaderImpl) GetAttribute(name string) interface{} {
	return h.Attributes[name]
}

func (h *HeaderImpl) SetAttribute(name string, value string) {
	h.Attributes[name] = value
}

func (h *HeaderImpl) SetStageCreator(stageCreator string) {
	h.StageCreator = stageCreator
}

func (h *HeaderImpl) SetSourceId(sourceId string) {
	h.SourceId = sourceId
}

func (h *HeaderImpl) SetTrackingId(trackingId string) {
	h.TrackingId = trackingId
}

func (h *HeaderImpl) SetPreviousTrackingId(previousTrackingId string) {
	h.PreviousTrackingId = previousTrackingId
}

func (h *HeaderImpl) SetStagesPath(stagesPath string) {
	h.StagesPath = stagesPath
}

func (h *HeaderImpl) SetErrorTimeStamp(timeStamp int64) {
	h.ErrorTimestamp = timeStamp
}

func (h *HeaderImpl) SetErrorStageInstance(errorStageInstance string) {
	h.ErrorStageInstance = errorStageInstance
}

func (h *HeaderImpl) SetErrorMessage(errorMessage string) {
	h.ErrorMessage = errorMessage
}

func (h *HeaderImpl) SetErrorPipelineName(errorPipelineName string) {
	h.ErrorPipelineName = errorPipelineName
}

func (h *HeaderImpl) SetErrorDataCollectorId(errorDataCollectorId string) {
	h.ErrorDataCollectorId = errorDataCollectorId
}

func (h *HeaderImpl) SetSourceRecord(sourceRecord api.Record) {
	h.sourceRecord = sourceRecord
}

func (h *HeaderImpl) clone() *HeaderImpl {
	clonedHeaderImpl := &HeaderImpl{Attributes: make(map[string]interface{})}
	for k, v := range h.GetAttributes() {
		clonedHeaderImpl.SetAttribute(k, v)
	}
	// Don't clone the source record
	clonedHeaderImpl.SetSourceRecord(h.sourceRecord)

	clonedHeaderImpl.SetSourceId(h.SourceId)
	clonedHeaderImpl.SetStageCreator(h.StageCreator)
	clonedHeaderImpl.SetStagesPath(h.GetStagesPath())
	clonedHeaderImpl.SetTrackingId(h.TrackingId)
	clonedHeaderImpl.SetPreviousTrackingId(h.PreviousTrackingId)
	return clonedHeaderImpl
}

func createRecord(recordSourceId string, value interface{}) (api.Record, error) {
	var rootField *api.Field
	var err error

	if value != nil {
		rootField, err = api.CreateField(value)
		if err != nil {
			return nil, err
		}
	}

	headerImpl := &HeaderImpl{Attributes: make(map[string]interface{})}
	r := &RecordImpl{
		header: headerImpl,
		value:  rootField,
	}
	headerImpl.SetSourceId(recordSourceId)
	return r, nil
}

func AddStageToStagePath(header *HeaderImpl, stageInstanceName string) {
	currentPath := ""
	if len(header.GetStagesPath()) > 0 {
		currentPath = header.GetStagesPath() + ":"
	}
	header.SetStagesPath(currentPath + stageInstanceName)
}

func CreateTrackingId(header *HeaderImpl) {
	currentTrackingID := header.GetTrackingId()
	newTrackingID := header.GetSourceId() + "::" + header.GetStagesPath()
	if len(currentTrackingID) > 0 {
		header.SetPreviousTrackingId(currentTrackingID)
	}
	header.SetTrackingId(newTrackingID)
}

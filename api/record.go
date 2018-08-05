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

package api

// Record represents the unit of data Data Collector Edge pipelines process.
//
// GetHeader method returns the metadata header of the record.
//
// Get method returns the root data field of the record.
type Record interface {
	GetHeader() Header
	Get(fieldPath ...string) (*Field, error)
	Set(field *Field) *Field
	SetField(fieldPath string, field *Field) (*Field, error)
	GetFieldPaths() map[string]bool
	Delete(fieldPath string) (*Field, error)
	Clone() Record
}

//Header represents metadata about the record
type Header interface {
	GetStageCreator() string

	GetSourceId() string

	GetTrackingId() string

	GetPreviousTrackingId() string

	GetStagesPath() string

	GetErrorDataCollectorId() string

	GetErrorPipelineName() string

	GetErrorMessage() string

	GetErrorStage() string

	GetErrorTimestamp() int64

	GetAttributeNames() []string

	GetAttributes() map[string]string

	GetAttribute(name string) interface{}

	SetAttribute(name string, value string)
}

const (
	EventRecordHeaderType              = "sdc.event.type"
	EventRecordHeaderVersion           = "sdc.event.version"
	EventRecordHeaderCreationTimestamp = "sdc.event.creation_timestamp"
)

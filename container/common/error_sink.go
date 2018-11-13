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
	"github.com/streamsets/datacollector-edge/api"
)

type ErrorSink struct {
	stageErrorMessages map[string][]api.ErrorMessage
	stageErrorRecords  map[string][]api.Record
	totalErrorRecords  int64
	totalErrorMessages int64
}

func NewErrorSink() *ErrorSink {
	errorSink := &ErrorSink{}
	errorSink.ClearErrorRecordsAndMessages()
	errorSink.totalErrorMessages = 0
	errorSink.totalErrorRecords = 0
	return errorSink
}

//After each batch call this function to clear current batch error messages/records
func (e *ErrorSink) ClearErrorRecordsAndMessages() {
	e.stageErrorMessages = make(map[string][]api.ErrorMessage)
	e.stageErrorRecords = make(map[string][]api.Record)
	e.totalErrorMessages = 0
	e.totalErrorRecords = 0
}

func (e *ErrorSink) GetStageErrorMessages(stageIns string) []api.ErrorMessage {
	return e.stageErrorMessages[stageIns]
}

func (e *ErrorSink) GetStageErrorRecords(stageIns string) []api.Record {
	return e.stageErrorRecords[stageIns]
}

func (e *ErrorSink) GetTotalErrorMessages() int64 {
	return e.totalErrorMessages
}

func (e *ErrorSink) GetTotalErrorRecords() int64 {
	return e.totalErrorRecords
}

func (e *ErrorSink) GetErrorRecords() map[string][]api.Record {
	return e.stageErrorRecords
}

func (e *ErrorSink) GetErrorMessages() map[string][]api.ErrorMessage {
	return e.stageErrorMessages
}

func (e *ErrorSink) ReportError(stageIns string, errorMessage api.ErrorMessage) {
	var errorMessages []api.ErrorMessage
	var keyExists bool
	errorMessages, keyExists = e.stageErrorMessages[stageIns]

	if !keyExists {
		errorMessages = make([]api.ErrorMessage, 0)
	}

	errorMessages = append(errorMessages, errorMessage)
	e.stageErrorMessages[stageIns] = errorMessages
	e.totalErrorMessages += 1
}

func (e *ErrorSink) ToError(stageIns string, record api.Record) {
	var errorRecords []api.Record
	var keyExists bool
	errorRecords, keyExists = e.stageErrorRecords[stageIns]

	if !keyExists {
		errorRecords = []api.Record{}
	}
	errorRecords = append(errorRecords, record)
	e.stageErrorRecords[stageIns] = errorRecords
	e.totalErrorRecords += 1
}

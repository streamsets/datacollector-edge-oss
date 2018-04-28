/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package common

import (
	"github.com/streamsets/datacollector-edge/api"
)

type ErrorSink struct {
	stageErrorMessages map[string][]error
	stageErrorRecords  map[string][]api.Record
	totalErrorRecords  int64
	totalErrorMessages int64
}

func NewErrorSink() *ErrorSink {
	errorSink := &ErrorSink{}
	errorSink.ClearErrorRecordsAndMesssages()
	errorSink.totalErrorMessages = 0
	errorSink.totalErrorRecords = 0
	return errorSink
}

//After each batch call this function to clear current batch error messages/records
func (e *ErrorSink) ClearErrorRecordsAndMesssages() {
	e.stageErrorMessages = make(map[string][]error)
	e.stageErrorRecords = make(map[string][]api.Record)
	e.totalErrorMessages = 0;
	e.totalErrorRecords = 0;
}

func (e *ErrorSink) GetStageErrorMessages(stageIns string) []error {
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

func (e *ErrorSink) ReportError(stageIns string, err error) {
	var errorMessages []error
	var keyExists bool
	errorMessages, keyExists = e.stageErrorMessages[stageIns]

	if !keyExists {
		errorMessages = []error{}
	}

	errorMessages = append(errorMessages, err)
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

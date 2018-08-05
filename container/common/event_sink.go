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

type EventSink struct {
	eventRecords map[string][]api.Record
}

func NewEventSink() *EventSink {
	eventSink := &EventSink{}
	eventSink.ClearEventRecords()
	return eventSink
}

func (e *EventSink) ClearEventRecords() {
	e.eventRecords = make(map[string][]api.Record)
}

func (e *EventSink) GetStageEvents(stageIns string) []api.Record {
	return e.eventRecords[stageIns]
}

func (e *EventSink) AddEvent(stageIns string, record api.Record) {
	var eventRecords []api.Record
	var keyExists bool
	eventRecords, keyExists = e.eventRecords[stageIns]

	if !keyExists {
		eventRecords = []api.Record{}
	}
	eventRecords = append(eventRecords, record)
	e.eventRecords[stageIns] = eventRecords
}

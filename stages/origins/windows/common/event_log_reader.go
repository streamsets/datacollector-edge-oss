// +build 386 windows,amd64 windows

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

type EventLogReader interface {
	Open() error
	Read(maxRecords int) ([]api.Record, error)
	GetCurrentOffset() string
	Close() error
}

type BaseEventLogReader struct {
	Log  string
	Mode EventLogReaderMode
}

type EventLogReaderMode string
type EventLogReaderAPIType string

const (
	ReadAll                      = EventLogReaderMode("ALL")
	ReadNew                      = EventLogReaderMode("NEW")
	ReaderAPITypeEventLogging    = EventLogReaderAPIType("EVENT_LOGGING")
	ReaderAPITypeWindowsEventLog = EventLogReaderAPIType("WINDOWS_EVENT_LOG")
)

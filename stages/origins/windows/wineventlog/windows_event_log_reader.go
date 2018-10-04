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
package wineventlog

import (
	"github.com/AllenDang/w32"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	wincommon "github.com/streamsets/datacollector-edge/stages/origins/windows/common"
)

type WindowsEventLogReader struct {
	*common.BaseStage
	*wincommon.BaseEventLogReader
	offset string
	handle w32.HANDLE
}

func (welr *WindowsEventLogReader) Open() error {
	log.Fatal("Unsupported") //TODO
	return nil
}

func (welr *WindowsEventLogReader) Read(maxRecords int) ([]api.Record, error) {
	records := make([]api.Record, 0)
	log.Fatal("Unsupported") //TODO
	return records, nil
}

func (welr *WindowsEventLogReader) GetCurrentOffset() string {
	log.Fatal("Unsupported") //TODO
	return ""
}

func (welr *WindowsEventLogReader) Close() error {
	log.Fatal("Unsupported") //TODO
	return nil
}

func NewWindowsEventLogReader(
	baseStage *common.BaseStage,
	logName string,
	mode wincommon.EventLogReaderMode,
	lastSourceOffset string,
) (*WindowsEventLogReader, error) {
	return &WindowsEventLogReader{
		BaseStage:          baseStage,
		BaseEventLogReader: &wincommon.BaseEventLogReader{Log: logName, Mode: mode},
		offset:             lastSourceOffset,
	}, nil
}

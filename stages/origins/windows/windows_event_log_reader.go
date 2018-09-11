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

package windows

import (
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"runtime"
	"strconv"
)

const (
	Library   = "streamsets-datacollector-windows-lib"
	StageName = "com_streamsets_pipeline_stage_origin_windows_WindowsEventLogDSource"
	Windows   = "windows"
)

type WindowsEventLogSource struct {
	*common.BaseStage
	LogName            string `ConfigDef:"type=STRING,required=true"`
	ReadMode           string `ConfigDef:"type=STRING,required=true"`
	CustomLogName      string `ConfigDef:"type=STRING,required=true"`
	resolvedLogName    string
	eventLogReaderMode EventLogReaderMode
	eventLogReader     *EventLogReader
}

func init() {
	stagelibrary.SetCreator(Library, StageName, func() api.Stage {
		return &WindowsEventLogSource{BaseStage: &common.BaseStage{}}
	})
}

func (wel *WindowsEventLogSource) Init(stageContext api.StageContext) []validation.Issue {
	issues := wel.BaseStage.Init(stageContext)

	if runtime.GOOS != Windows {
		issues = append(issues, stageContext.CreateConfigIssue(
			"Windows Event Log Source should be run on Windows OS",
		))
		return issues
	}

	if !(wel.LogName == SYSTEM || wel.LogName == APPLICATION || wel.LogName == SECURITY || wel.LogName == Custom) {
		issues = append(issues, stageContext.CreateConfigIssue("Unsupported Log Name :"+wel.LogName))
		return issues
	}

	if wel.LogName == Custom {
		wel.resolvedLogName = wel.CustomLogName
	} else {
		wel.resolvedLogName = wel.LogName
	}

	wel.eventLogReaderMode = EventLogReaderMode(wel.ReadMode)

	return issues
}

func (wel *WindowsEventLogSource) Produce(
	lastSourceOffset *string,
	maxBatchSize int,
	batchMaker api.BatchMaker,
) (*string, error) {
	if wel.eventLogReader == nil {
		if lastSourceOffset == nil || *lastSourceOffset == "" {
			wel.eventLogReader = NewReader(wel.resolvedLogName, wel.eventLogReaderMode, 0, false)
		} else {
			off, err := strconv.ParseUint(*lastSourceOffset, 10, 32)
			if err != nil {
				wel.GetStageContext().ReportError(err)
				log.WithError(err).WithField("offset", lastSourceOffset).Error("Error while parsing offset")
				return lastSourceOffset, err
			}
			wel.eventLogReader = NewReader(wel.resolvedLogName, wel.eventLogReaderMode, uint32(off), true)
		}
		if err := wel.eventLogReader.Open(); err != nil {
			wel.GetStageContext().ReportError(err)
			log.WithError(err).Error("Error while opening event reader")
			return lastSourceOffset, err
		}
	}

	if events, err := wel.eventLogReader.Read(maxBatchSize); err == nil {
		if len(events) > 0 {
			for _, event := range events {
				er := wel.createRecordAndAddToBatch(event, batchMaker)
				if er != nil {
					log.WithError(er).Error("Error when creating record")
					wel.GetStageContext().ReportError(er)
					return lastSourceOffset, er
				}
			}
		}
	} else {
		wel.GetStageContext().ReportError(err)
		log.WithError(err).Error("Error on event log read")
		return lastSourceOffset, err
	}

	newOffset := strconv.FormatUint(uint64(wel.eventLogReader.GetCurrentOffset()), 10)

	return &newOffset, nil
}

func (wel *WindowsEventLogSource) createRecordAndAddToBatch(event EventLogRecord, batchMaker api.BatchMaker) error {
	recordId := event.ComputerName + "::" + wel.resolvedLogName + "::" +
		strconv.FormatUint(uint64(event.RecordNumber), 10) + "::" +
		strconv.FormatUint(uint64(event.TimeGenerated), 10)
	recordVal := map[string]interface{}{
		"ComputerName":  event.ComputerName,
		"RecordNumber":  event.RecordNumber,
		"DataOffset":    event.DataOffset,
		"DataLength":    event.DataLength,
		"Category":      event.EventCategory,
		"EventId":       event.EventID,
		"EventType":     event.EventType,
		"SourceName":    event.SourceName,
		"LogName":       wel.resolvedLogName,
		"StringOffset":  event.StringOffset,
		"Reserved":      event.Reserved,
		"TimeGenerated": event.TimeGenerated,
		"TimeWritten":   event.TimeWritten,
		"ReservedFlags": event.ReservedFlags,
		"UserSidLength": event.UserSidLength,
		"UserSidOffset": event.UserSidOffset,
		"NumStrings":    event.NumStrings,
		"Length":        event.Length,
		"MsgStrings":    event.MsgStrings,
		"Message":       event.Message,
	}
	record, er := wel.GetStageContext().CreateRecord(recordId, recordVal)
	if er != nil {
		return er
	}
	batchMaker.AddRecord(record)
	return nil
}

func (wel *WindowsEventLogSource) Destroy() error {
	if wel.eventLogReader != nil {
		err := wel.eventLogReader.Close()
		if err != nil {
			log.WithError(err).Error("Error closing event reader")
		}
	}
	ReleaseResourceLibraries()
	return nil
}

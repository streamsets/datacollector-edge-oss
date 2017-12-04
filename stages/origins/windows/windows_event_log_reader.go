// +build 386 windows,amd64 windows

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

package windows

import (
	"errors"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"runtime"
	"strconv"
)

const (
	LIBRARY          = "streamsets-datacollector-windows-lib"
	STAGE_NAME       = "com_streamsets_pipeline_stage_origin_windows_WindowsEventLogDSource"
	WINDOWS          = "windows"
	LOG_NAME_CONFIG  = "logName"
	READ_MODE_CONFIG = "readMode"
)

type WindowsEventLogSource struct {
	*common.BaseStage
	logName        string
	readMode       EventLogReaderMode
	eventLogReader *EventLogReader
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &WindowsEventLogSource{BaseStage: &common.BaseStage{}}
	})
}

func (wel *WindowsEventLogSource) Init(stageContext api.StageContext) error {
	if err := wel.BaseStage.Init(stageContext); err != nil {
		return err
	}
	stageConfig := wel.GetStageConfig()

	if runtime.GOOS != WINDOWS {
		return errors.New("Windows Event Log Source should be run on Windows OS")
	}

	for _, config := range stageConfig.Configuration {
		value, err := wel.GetStageContext().GetResolvedValue(config.Value)
		if err != nil {
			return err
		}
		switch config.Name {
		case LOG_NAME_CONFIG:
			logName := value.(string)
			if !(logName == SYSTEM || logName == APPLICATION || logName == SECURITY) {
				return errors.New("Unsupported Log Name :" + logName)
			}
			wel.logName = logName
		case READ_MODE_CONFIG:
			wel.readMode = EventLogReaderMode(value.(string))
		}
	}
	return nil
}

func (wel *WindowsEventLogSource) Produce(
	lastSourceOffset string,
	maxBatchSize int,
	batchMaker api.BatchMaker,
) (string, error) {
	if wel.eventLogReader == nil {
		if lastSourceOffset == "" {
			wel.eventLogReader = NewReader(wel.logName, wel.readMode, 0, false)
		} else {
			off, err := strconv.ParseUint(lastSourceOffset, 10, 32)
			if err != nil {
				wel.GetStageContext().ReportError(err)
				log.WithError(err).WithField("offset", lastSourceOffset).Error("Error while parsing offset")
				return lastSourceOffset, err
			}
			wel.eventLogReader = NewReader(wel.logName, wel.readMode, uint32(off), true)
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

	return strconv.FormatUint(uint64(wel.eventLogReader.GetCurrentOffset()), 10), nil
}

func (wel *WindowsEventLogSource) createRecordAndAddToBatch(event EventLogRecord, batchMaker api.BatchMaker) error {
	recordId := event.ComputerName + "::" + wel.logName + "::" + string(event.EventID)
	recordVal := map[string]interface{}{
		"ComputerName":  event.ComputerName,
		"RecordNumber":  event.RecordNumber,
		"DataOffset":    event.DataOffset,
		"DataLength":    event.DataLength,
		"Category":      event.EventCategory,
		"EventId":       event.EventID,
		"SourceName":    event.SourceName,
		"LogName":       wel.logName,
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
	err := wel.eventLogReader.Close()
	if err != nil {
		log.WithError(err).Error("Error closing event reader")
	}
	ReleaseResourceLibraries()
	return nil
}

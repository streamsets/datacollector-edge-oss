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
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	wincommon "github.com/streamsets/datacollector-edge/stages/origins/windows/common"
	"github.com/streamsets/datacollector-edge/stages/origins/windows/eventlogging"
	"github.com/streamsets/datacollector-edge/stages/origins/windows/wineventlog"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"runtime"
	"strconv"
)

const (
	Library       = "streamsets-datacollector-windows-lib"
	StageName     = "com_streamsets_pipeline_stage_origin_windows_WindowsEventLogDSource"
	Windows       = "windows"
	OffsetVersion = 1
)

const (
	Application = "Application"
	System      = "System"
	Security    = "Security"
	Custom      = "Custom"
)

type WindowsEventLogSource struct {
	*common.BaseStage
	ReaderAPIType         string                    `ConfigDef:"type=STRING,required=true"`
	CommonConf            wincommon.CommonConf      `ConfigDefBean:"name=commonConf"`
	WinEventLogConf       wincommon.WinEventLogConf `ConfigDefBean:"name=winEventLogConf"`
	bufferSize            int
	resolvedLogName       string
	eventLogReaderMode    wincommon.EventLogReaderMode
	eventLogReaderAPIType wincommon.EventLogReaderAPIType
	eventLogReader        wincommon.EventLogReader
	offset                *WindowsEventLogOffset
}

type WindowsEventLogOffset struct {
	EventLogReaderAPIType wincommon.EventLogReaderAPIType
	OffsetVersion         uint8
	Offset                string
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

	if !(wel.CommonConf.LogName == System || wel.CommonConf.LogName == Application ||
		wel.CommonConf.LogName == Security || wel.CommonConf.LogName == Custom) {
		issues = append(issues, stageContext.CreateConfigIssue("Unsupported Log Name :"+wel.CommonConf.LogName))
		return issues
	}

	wel.bufferSize = int(wel.CommonConf.BufferSize)

	wel.resolvedLogName = wel.CommonConf.LogName
	if wel.CommonConf.LogName == Custom {
		wel.resolvedLogName = wel.CommonConf.CustomLogName
	}

	wel.eventLogReaderMode = wincommon.EventLogReaderMode(wel.CommonConf.ReadMode)
	wel.eventLogReaderAPIType = wincommon.EventLogReaderAPIType(wel.ReaderAPIType)
	return issues
}

func NewReader(
	baseStage *common.BaseStage,
	logReaderType wincommon.EventLogReaderAPIType,
	logName string,
	mode wincommon.EventLogReaderMode,
	bufferSize int,
	maxBatchSize int,
	initialOffset string,
	winEventLogConf wincommon.WinEventLogConf,
) (wincommon.EventLogReader, error) {
	if logReaderType == wincommon.ReaderAPITypeEventLogging {
		return eventlogging.NewEventLoggingReader(
			baseStage,
			logName,
			mode,
			bufferSize,
			maxBatchSize,
			initialOffset,
		)
	} else {
		return wineventlog.NewWindowsEventLogReader(
			baseStage,
			logName,
			mode,
			bufferSize,
			maxBatchSize,
			initialOffset,
			winEventLogConf,
		)
	}
}

func (wel *WindowsEventLogSource) Produce(
	lastSourceOffset *string,
	maxBatchSize int,
	batchMaker api.BatchMaker,
) (*string, error) {
	var err error
	// Read offset if it is not present
	if wel.offset == nil {
		if wel.offset, err = wel.extractAndUpgradeOffsetIfNeeded(lastSourceOffset); err != nil {
			log.WithError(err).Error("Error reading offset")
			return lastSourceOffset, err
		}
	}

	if wel.eventLogReader == nil {
		if wel.eventLogReader, err = NewReader(
			wel.BaseStage,
			wel.eventLogReaderAPIType,
			wel.resolvedLogName,
			wel.eventLogReaderMode,
			wel.bufferSize,
			maxBatchSize,
			wel.offset.Offset,
			wel.WinEventLogConf,
		); err == nil {
			err = wel.eventLogReader.Open()
		}
		if err != nil {
			wel.GetStageContext().ReportError(err)
			log.WithError(err).Error("Error while opening event reader")
			return lastSourceOffset, err
		}
	}

	if eventRecords, err := wel.eventLogReader.Read(); err == nil {
		if len(eventRecords) > 0 {
			for _, eventRecord := range eventRecords {
				batchMaker.AddRecord(eventRecord)
			}
		}
	} else {
		wel.GetStageContext().ReportError(err)
		log.WithError(err).Error("Error on event log read")
		return lastSourceOffset, err
	}

	wel.offset.Offset = wel.eventLogReader.GetCurrentOffset()

	var offsetString string
	if offsetBytes, err := json.Marshal(wel.offset); err == nil {
		offsetString = string(offsetBytes)
	} else {
		log.WithError(err).Errorf("Error Marshaling offset : %s", wel.eventLogReader.GetCurrentOffset())
	}
	return &offsetString, nil
}

func (wel *WindowsEventLogSource) Destroy() error {
	if wel.eventLogReader != nil {
		if err := wel.eventLogReader.Close(); err != nil {
			log.WithError(err).Error("Error closing event reader")
		}
	}
	return nil
}

//private methods
func (wel *WindowsEventLogSource) extractAndUpgradeOffsetIfNeeded(offsetStringPtr *string) (*WindowsEventLogOffset, error) {
	if offsetStringPtr == nil || *offsetStringPtr == "" {
		return &WindowsEventLogOffset{
			EventLogReaderAPIType: wel.eventLogReaderAPIType,
			OffsetVersion:         OffsetVersion,
			Offset:                *offsetStringPtr,
		}, nil
	} else {
		var welo WindowsEventLogOffset
		err := json.Unmarshal([]byte(*offsetStringPtr), &welo)
		if err != nil {
			log.WithField("offset", *offsetStringPtr).WithError(err).Debug(
				"Not able to deserialize the offset assuming no offset version/Event log reader type present")
			// Try decoding the value as uint32
			_, err := strconv.ParseUint(*offsetStringPtr, 10, 32)
			if err != nil {
				log.WithError(err).Error("Not able to deserialize the offset to uint32")
				return nil, err
			} else {
				if wel.eventLogReaderAPIType != wincommon.ReaderAPITypeEventLogging {
					return nil, errors.New(
						fmt.Sprintf(
							"looks like the offset is from %s but the origin"+
								" configuration for event reader type is %s,"+
								" please reset the offset if you want to use %s",
							wincommon.ReaderAPITypeEventLogging,
							wincommon.ReaderAPITypeWindowsEventLog,
							wincommon.ReaderAPITypeWindowsEventLog,
						),
					)
				}
				return &WindowsEventLogOffset{
					EventLogReaderAPIType: wel.eventLogReaderAPIType,
					OffsetVersion:         OffsetVersion,
					Offset:                *offsetStringPtr,
				}, nil
			}
		}
		return &welo, nil
	}

}

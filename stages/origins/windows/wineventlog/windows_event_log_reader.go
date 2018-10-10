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
	"fmt"
	"github.com/AllenDang/w32"
	"github.com/clbanning/mxj"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	wincommon "github.com/streamsets/datacollector-edge/stages/origins/windows/common"
	"time"
)

type WindowsEventLogReader struct {
	*common.BaseStage
	*wincommon.BaseEventLogReader
	eventSubscriber WinEventSubscriber
	offset          string
	handle          w32.HANDLE
}

func (welr *WindowsEventLogReader) Open() error {
	err := welr.eventSubscriber.Subscribe()
	if err != nil {
		log.WithError(err).Error("Error subscribing")
	}
	return err
}

func (welr *WindowsEventLogReader) Read() ([]api.Record, error) {
	records := make([]api.Record, 0)
	eventStrings, err := welr.eventSubscriber.Read()
	if err == nil {
		for _, eventString := range eventStrings {
			parsedXmlMap, err := mxj.NewMapXml([]byte(eventString))
			if err == nil {
				//TODO
				record, err := welr.GetStageContext().CreateRecord("random", map[string]interface{}(parsedXmlMap))
				if err != nil {
					log.WithError(err).Errorf("Error creating record with parsedXml : %v", parsedXmlMap)
					welr.GetStageContext().ReportError(err)
				}
				records = append(records, record)
			} else {
				log.WithError(err).Errorf("Error converting string %s to xml", eventString)
				record, err := welr.GetStageContext().CreateRecord("random", eventString)
				if err != nil {
					log.WithError(err).Errorf("Error creating record for error")
					welr.GetStageContext().ReportError(err)
				} else {
					welr.GetStageContext().ToError(err, record)
				}
			}
		}
	} else {
		log.WithError(err).Error("Error reading from windows event log")
	}
	return records, err
}

func (welr *WindowsEventLogReader) GetCurrentOffset() string {
	return welr.eventSubscriber.GetBookmark()
}

func (welr *WindowsEventLogReader) Close() error {
	welr.eventSubscriber.Close()
	return nil
}

func NewWindowsEventLogReader(
	baseStage *common.BaseStage,
	logName string,
	mode wincommon.EventLogReaderMode,
	bufferSize int,
	maxBatchSize int,
	lastSourceOffset string,
	winEventLogConf wincommon.WinEventLogConf,
) (*WindowsEventLogReader, error) {
	subscriptionMode := SubscriptionMode(winEventLogConf.SubscriptionMode)
	query := fmt.Sprintf(`<QueryList> <Query Id="0"> <Select Path="%s">*</Select> </Query></QueryList>`, logName)

	log.Debugf("Wait time seconds %d, duration %d",
		int64(winEventLogConf.MaxWaitTimeSecs),
		time.Duration(int64(winEventLogConf.MaxWaitTimeSecs))*time.Second)

	return &WindowsEventLogReader{
		BaseStage:          baseStage,
		BaseEventLogReader: &wincommon.BaseEventLogReader{Log: logName, Mode: mode},
		eventSubscriber: NewWinEventSubscriber(
			subscriptionMode,
			query,
			uint32(maxBatchSize),
			lastSourceOffset,
			mode,
			bufferSize,
			time.Duration(int64(winEventLogConf.MaxWaitTimeSecs))*time.Second,
		),
		offset: lastSourceOffset,
	}, nil
}

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
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	wincommon "github.com/streamsets/datacollector-edge/stages/origins/windows/common"
	winevtsubscription "github.com/streamsets/datacollector-edge/stages/origins/windows/wineventlog/subscription"
	winevtrender "github.com/streamsets/datacollector-edge/stages/origins/windows/wineventlog/subscription/rendering"
	"time"
)

type windowsEventLogReader struct {
	*common.BaseStage
	*wincommon.BaseEventLogReader
	eventSubscriber winevtsubscription.WinEventSubscriber
	offset          string
	handle          w32.HANDLE
}

func (welr *windowsEventLogReader) Open() error {
	err := welr.eventSubscriber.Subscribe()
	if err != nil {
		log.WithError(err).Error("Error subscribing")
	}
	return err
}

func (welr *windowsEventLogReader) Read() ([]api.Record, error) {
	eventRecords, err := welr.eventSubscriber.GetRecords()
	if err != nil {
		log.WithError(err).Error("Error reading from windows event log")
	}
	return eventRecords, err
}

func (welr *windowsEventLogReader) GetCurrentOffset() string {
	return welr.eventSubscriber.GetBookmark()
}

func (welr *windowsEventLogReader) Close() error {
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
) (wincommon.EventLogReader, error) {
	subscriptionMode := winevtsubscription.SubscriptionMode(winEventLogConf.SubscriptionMode)
	rawEventPopulationStrategy := winevtrender.RawEventPopulationStrategy(winEventLogConf.RawEventPopulationStrategy)

	query := fmt.Sprintf(`<QueryList> <Query Id="0"> <Select Path="%s">*</Select> </Query></QueryList>`, logName)
	log.Debugf("Querying windows Event log with %s", logName)
	return &windowsEventLogReader{
		BaseStage:          baseStage,
		BaseEventLogReader: &wincommon.BaseEventLogReader{Log: logName, Mode: mode},
		eventSubscriber: winevtsubscription.NewWinEventSubscriber(
			baseStage.GetStageContext(),
			subscriptionMode,
			rawEventPopulationStrategy,
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

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
package subscription

import (
	"github.com/Workiva/go-datastructures/queue"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/stages/origins/windows/common"
	winevtcommon "github.com/streamsets/datacollector-edge/stages/origins/windows/wineventlog/common"
	winevtrender "github.com/streamsets/datacollector-edge/stages/origins/windows/wineventlog/subscription/rendering"
	"golang.org/x/sys/windows"
	"time"
)

type SubscriptionMode string

const (
	PushSubscription = SubscriptionMode("PUSH")
	PullSubscription = SubscriptionMode("PULL")
)

type WinEventSubscriber interface {
	/** Subscribe to Win Event Log
	 */
	Subscribe() error
	/** Read a singl event
	 */
	GetRecords() ([]api.Record, error)

	/** Get BookmarkXML
	 */
	GetBookmark() string

	/** Close Subscription
	 */
	Close()
}

type baseWinEventSubscriber struct {
	stageContext         api.StageContext
	eventsQueue          *queue.Queue
	query                string
	maxNoOfEvents        uint32
	subscriptionHandle   winevtcommon.SubscriptionHandle
	bookMark             string
	eventReaderMode      common.EventLogReaderMode
	subscriptionCallback winevtcommon.EvtSubscribeCallback
	signalEventHandle    windows.Handle
	maxWaitTime          time.Duration
	bookMarkHandle       winevtcommon.BookmarkHandle
	renderer             *winevtrender.WinEventLogRenderer
}

func (bwes *baseWinEventSubscriber) getBookmarkHandleAndFlags() (
	winevtcommon.BookmarkHandle,
	winevtcommon.EvtSubscribeFlag,
	error,
) {
	var err error
	//If offset present use the offset
	if bwes.bookMark != "" {
		//Create a bookmark handle for bookMarkXML and return that handle for subscription
		bwes.bookMarkHandle, err = winevtcommon.EvtCreateBookmark(bwes.bookMark)
		if err != nil {
			log.WithError(err).Errorf("Error creating bookmark with bookmark XML: %s", bwes.bookMark)
		}
		return bwes.bookMarkHandle, winevtcommon.EvtSubscribeStartAfterBookmark, err
	} else {
		//No bookmark offset present
		flags := winevtcommon.EvtSubscribeToFutureEvents
		if bwes.eventReaderMode == common.ReadAll {
			//If no offset use Start from oldest record if ReadAll or else use Only Future Events (i.e Read New)
			flags = winevtcommon.EvtSubscribeStartAtOldestRecord
		}
		//Create empty bookmark
		bwes.bookMarkHandle, err = winevtcommon.EvtCreateBookmark("")
		return 0, flags, err
	}
}

func (bwes *baseWinEventSubscriber) Subscribe() error {
	var err error
	bookmarkHandle, flags, err := bwes.getBookmarkHandleAndFlags()
	if err == nil {
		if bwes.subscriptionHandle, err = winevtcommon.EvtSubscribe(
			bwes.signalEventHandle,
			"",
			bwes.query,
			bookmarkHandle,
			bwes.subscriptionCallback,
			flags,
		); err != nil {
			switch err {
			case winevtcommon.ErrorEvtChannelNotFound:
				log.WithError(err).Errorf("Channel not found %s", "Security")
			case winevtcommon.ErrorInvalidQuery:
				log.WithError(err).Error("Query is not valid")
			default:
				log.WithError(err).Error("Event subscribe failed")
			}
		}
	} else {
		log.WithError(err).Errorf("Error determining bookmark and subscription flags")
	}
	return err
}

func (bwes *baseWinEventSubscriber) GetRecords() ([]api.Record, error) {
	var err error
	eventRecords := make([]api.Record, 0)
	if !bwes.eventsQueue.Empty() {
		var eventRecordsFromQueue []interface{}
		eventRecordsFromQueue, err = bwes.eventsQueue.Poll(int64(bwes.maxNoOfEvents), bwes.maxWaitTime)
		if err == queue.ErrTimeout {
			log.Debugf("Windows Event Log Queue wait time out, no events")
			err = nil
		} else if err == nil {
			if len(eventRecordsFromQueue) > 0 {
				for _, eventRecordFromQueue := range eventRecordsFromQueue {
					eventRecords = append(eventRecords, eventRecordFromQueue.(api.Record))
				}
				bwes.bookMark, err = bwes.renderer.RenderBookmark(bwes.bookMarkHandle)
				if err != nil {
					log.WithError(err).Errorf("Error rendering bookmark xml")
				}
			}
		} else {
			log.WithError(err).Error("Error happened when polling from queue")
		}
	} else {
		log.Debugf("Windows Event Log Queue is empty")
	}
	return eventRecords, err
}

func (bwes *baseWinEventSubscriber) GetBookmark() string {
	return bwes.bookMark
}

func (bwes *baseWinEventSubscriber) Close() {
	if bwes.eventsQueue != nil {
		bwes.eventsQueue.Dispose()
	}
	if bwes.signalEventHandle != 0 {
		windows.CloseHandle(bwes.signalEventHandle)
	}
	if bwes.subscriptionHandle != 0 {
		bwes.subscriptionHandle.Close()
	}
	if bwes.bookMarkHandle != 0 {
		bwes.bookMarkHandle.Close()
	}
	if bwes.renderer != nil {
		bwes.renderer.Close()
	}
}

func NewWinEventSubscriber(
	stageContext api.StageContext,
	subscriptionMode SubscriptionMode,
	rawEventPopulationStrategy winevtrender.RawEventPopulationStrategy,
	query string,
	maxNumberOfEvents uint32,
	bookMark string,
	eventReaderMode common.EventLogReaderMode,
	bufferSize int,
	maxWaitTime time.Duration,
) WinEventSubscriber {
	baseEventSubscriber := &baseWinEventSubscriber{
		stageContext:    stageContext,
		query:           query,
		maxNoOfEvents:   maxNumberOfEvents,
		eventsQueue:     queue.New(int64(maxNumberOfEvents)),
		bookMark:        bookMark,
		eventReaderMode: eventReaderMode,
		maxWaitTime:     maxWaitTime,
		renderer:        winevtrender.NewWinEventLogRenderer(bufferSize, rawEventPopulationStrategy),
	}

	if subscriptionMode == PushSubscription {
		return &pushWinEventSubscriber{
			baseWinEventSubscriber: baseEventSubscriber,
		}
	} else {
		return &pullWinEventSubscriber{
			baseWinEventSubscriber: baseEventSubscriber,
		}
	}
}

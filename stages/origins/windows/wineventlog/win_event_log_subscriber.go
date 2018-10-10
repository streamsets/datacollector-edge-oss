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
	"github.com/Workiva/go-datastructures/queue"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/stages/origins/windows/common"
	"golang.org/x/sys/windows"
	"syscall"
	"time"
	"unsafe"
)

type SubscriptionMode string

const (
	PushSubscription  = SubscriptionMode("PUSH")
	PullSubscription  = SubscriptionMode("PULL")
	BufferSizeDefault = uint32(8 * 1024)
)

type WinEventSubscriber interface {
	/** Subscribe to Win Event Log
	 */
	Subscribe() error
	/** Read a singl event
	 */
	Read() ([]string, error)

	/** Get BookmarkXML
	 */
	GetBookmark() string

	/** Close Subscription
	 */
	Close()
}

type BaseWinEventSubscriber struct {
	eventsQueue          *queue.Queue
	query                string
	maxNoOfEvents        uint32
	subscriptionHandle   SubscriptionHandle
	bookMark             string
	eventReaderMode      common.EventLogReaderMode
	subscriptionCallback EvtSubscribeCallback
	signalEventHandle    windows.Handle
	bufferSize           int
	bufferForRender      []byte
	//TODO : Batch wait time or poll time which can be exposed
	maxWaitTimeMillis int
}

func (bwes *BaseWinEventSubscriber) renderEventXML(eventHandle EventHandle) (string, error) {
	dwBufferUsed := uint32(0)
	dwPropertyCount := uint32(0)
	var eventString string
	err := EvtRender(EventHandle(0), eventHandle, EvtRenderEventXml, uint32(len(bwes.bufferForRender)),
		unsafe.Pointer(&bwes.bufferForRender[0]), &dwBufferUsed, &dwPropertyCount)
	if err != nil && err == ErrorInsufficientBuffer && bwes.bufferSize == -1 {
		log.Infof(
			"Insufficient Buffer with length: %d. Retrying with Buffer of size: %d",
			len(bwes.bufferForRender),
			dwBufferUsed,
		)
		bwes.bufferForRender = make([]byte, dwBufferUsed+1) //Creating a new buffer of size determined by
		err = EvtRender(EventHandle(0), eventHandle, EvtRenderEventXml, uint32(len(bwes.bufferForRender)),
			unsafe.Pointer(&bwes.bufferForRender[0]), &dwBufferUsed, &dwPropertyCount)
	}
	if err != nil {
		log.WithError(err).Errorf("Event Render Failed %d", err.(syscall.Errno))
		return "", err
	} else {
		eventBytes := bwes.bufferForRender[:dwBufferUsed]
		eventString, err = ExtractString(eventBytes)
		if err == nil {
			//Store last rendered event string
			bwes.bookMark = eventString
		}
	}
	return eventString, err
}

func (bwes *BaseWinEventSubscriber) Subscribe() error {
	var err error
	flags := EvtSubscribeToFutureEvents
	//If offset present use the offset
	if bwes.bookMark != "" {
		flags = EvtSubscribeStartAfterBookmark
		//TODO : Extract Bookmark from bookmark string and subscribe accordingly
	} else if bwes.eventReaderMode == common.ReadAll {
		//If no offset use Start from oldest record if ReadAll or else use Only Future Events (i.e Read New)
		flags = EvtSubscribeStartAtOldestRecord
	}
	if bwes.subscriptionHandle, err = EvtSubscribe(
		bwes.signalEventHandle,
		"",
		bwes.query,
		0,
		bwes.subscriptionCallback,
		flags,
	); err != nil {
		switch err {
		case ErrorEvtChannelNotFound:
			log.WithError(err).Errorf("Channel not found %s", "Security")
		case ErrorInvalidQuery:
			log.WithError(err).Error("Query is not valid")
		default:
			log.WithError(err).Error("Event subscribe failed")
		}
	}
	return err
}

func (bwes *BaseWinEventSubscriber) Read() ([]string, error) {
	var err error
	eventStrings := make([]string, 0)
	if !bwes.eventsQueue.Empty() {
		var vals []interface{}
		vals, err = bwes.eventsQueue.Poll(
			int64(bwes.maxNoOfEvents),
			time.Duration(bwes.maxWaitTimeMillis)*time.Millisecond)
		if err == nil || err == queue.ErrTimeout {
			for _, val := range vals {
				eventString := val.(string)
				eventStrings = append(eventStrings, eventString)
			}
		} else {
			log.WithError(err).Error("Error happened when polling from queue")
		}
	} else {
		log.Infof("Windows Event Log Queue is empty")
	}
	return eventStrings, err
}

func (bwes *BaseWinEventSubscriber) GetBookmark() string {
	return bwes.bookMark
}

func (bwes *BaseWinEventSubscriber) Close() {
	if bwes.eventsQueue != nil {
		bwes.eventsQueue.Dispose()
	}
	if bwes.signalEventHandle != 0 {
		windows.CloseHandle(bwes.signalEventHandle)
	}
	if bwes.subscriptionHandle != 0 {
		EvtClose(uintptr(bwes.subscriptionHandle))
	}
}

func NewWinEventSubscriber(
	subscriptionMode SubscriptionMode,
	query string,
	maxNumberOfEvents uint32,
	bookMark string,
	eventReaderMode common.EventLogReaderMode,
	bufferSize int,
	maxWaitTime time.Duration,
) WinEventSubscriber {
	bufferSizeValue := int(BufferSizeDefault)
	if bufferSize != -1 {
		bufferSizeValue = bufferSize
	}
	baseEventSubscriber := &BaseWinEventSubscriber{
		query:           query,
		maxNoOfEvents:   maxNumberOfEvents,
		eventsQueue:     queue.New(int64(maxNumberOfEvents)),
		bookMark:        bookMark,
		eventReaderMode: eventReaderMode,
		bufferSize:      bufferSize,
		//TODO: Expose Buffer Size
		bufferForRender: make([]byte, bufferSizeValue),
		//TODO: Expose Wait Time Config
		maxWaitTimeMillis: int(time.Duration(maxWaitTime / time.Microsecond)),
	}

	log.Infof("Wait time millis %d", baseEventSubscriber.maxWaitTimeMillis)

	if subscriptionMode == PushSubscription {
		return &PushWinEventSubscriber{
			BaseWinEventSubscriber: baseEventSubscriber,
		}
	} else {
		return &PullWinEventSubscriber{
			BaseWinEventSubscriber: baseEventSubscriber,
		}
	}
}

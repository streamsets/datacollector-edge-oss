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
	maxWaitTime          time.Duration
	bookMarkHandle       BookmarkHandle
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
		//Update Bookmark
		if err == nil {
			err = EvtUpdateBookmark(bwes.bookMarkHandle, eventHandle)
			if err != nil {
				log.WithError(err).Error("Error Updating bookmark")
			}
		}
	}
	return eventString, err
}

func (bwes *BaseWinEventSubscriber) renderAndUpdateBookmark() error {
	dwBufferUsed := uint32(0)
	dwPropertyCount := uint32(0)
	//Render bookmark XML
	err := EvtRender(
		EventHandle(0),
		EventHandle(bwes.bookMarkHandle),
		EvtRenderBookmark,
		uint32(len(bwes.bufferForRender)),
		unsafe.Pointer(&bwes.bufferForRender[0]),
		&dwBufferUsed,
		&dwPropertyCount,
	)
	if err == nil {
		bookMarkXmlBytes := bwes.bufferForRender[:dwBufferUsed]
		//Store bookmark xml for offset management
		bwes.bookMark, err = ExtractString(bookMarkXmlBytes)
	}
	return err
}

func (bwes *BaseWinEventSubscriber) getBookmarkHandleAndFlags() (BookmarkHandle, EvtSubscribeFlag, error) {
	var err error
	//If offset present use the offset
	if bwes.bookMark != "" {
		//Create a bookmark handle for bookMarkXML and return that handle for subscription
		bwes.bookMarkHandle, err = EvtCreateBookmark(bwes.bookMark)
		if err != nil {
			log.WithError(err).Errorf("Error creating bookmark with bookmark XML: %s", bwes.bookMark)
		}
		return bwes.bookMarkHandle, EvtSubscribeStartAfterBookmark, err
	} else {
		//No bookmark offset present
		flags := EvtSubscribeToFutureEvents
		if bwes.eventReaderMode == common.ReadAll {
			//If no offset use Start from oldest record if ReadAll or else use Only Future Events (i.e Read New)
			flags = EvtSubscribeStartAtOldestRecord
		}
		//Create empty bookmark
		bwes.bookMarkHandle, err = EvtCreateBookmark("")
		return 0, flags, err
	}
}

func (bwes *BaseWinEventSubscriber) Subscribe() error {
	var err error
	bookmarkHandle, flags, err := bwes.getBookmarkHandleAndFlags()
	if err == nil {
		if bwes.subscriptionHandle, err = EvtSubscribe(
			bwes.signalEventHandle,
			"",
			bwes.query,
			bookmarkHandle,
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
	} else {
		log.WithError(err).Errorf("Error determining bookmark and subscription flags")
	}
	return err
}

func (bwes *BaseWinEventSubscriber) Read() ([]string, error) {
	var err error
	eventStrings := make([]string, 0)
	if !bwes.eventsQueue.Empty() {
		var vals []interface{}
		vals, err = bwes.eventsQueue.Poll(int64(bwes.maxNoOfEvents), bwes.maxWaitTime)
		if err == queue.ErrTimeout {
			log.Debugf("Windows Event Log Queue wait time out, no events")
			err = nil
		} else if err == nil {
			if len(vals) > 0 {
				for _, val := range vals {
					eventString := val.(string)
					eventStrings = append(eventStrings, eventString)
				}
				err = bwes.renderAndUpdateBookmark()
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
		log.Debug("Closing Signal Handle")
		windows.CloseHandle(bwes.signalEventHandle)
	}
	if bwes.subscriptionHandle != 0 {
		log.Debug("Closing Subscription Handle")
		EvtClose(uintptr(bwes.subscriptionHandle))
	}
	if bwes.bookMarkHandle != 0 {
		log.Debug("Closing Bookmark Handle")
		EvtClose(uintptr(bwes.bookMarkHandle))
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
		bufferForRender: make([]byte, bufferSizeValue),
		maxWaitTime:     maxWaitTime,
	}

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

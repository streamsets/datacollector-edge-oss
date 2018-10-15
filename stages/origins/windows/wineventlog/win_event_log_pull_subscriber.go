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
	"github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"golang.org/x/sys/windows"
	"syscall"
	"time"
)

type PullWinEventSubscriber struct {
	*BaseWinEventSubscriber
}

func (pwes *PullWinEventSubscriber) fetchEventsImmediately() error {
	var err error
	for pwes.eventsQueue.Len() < int64(pwes.maxNoOfEvents) {
		fetchedEventHandles := make([]EventHandle, 1)
		returnedHandles := uint32(0)
		err := EvtNext(pwes.subscriptionHandle, uint32(1), fetchedEventHandles, &returnedHandles)
		if err == nil {
			//log.Debugf("Fetched %d event handles", returnedHandles)
			for _, fetchedEventHandle := range fetchedEventHandles[:returnedHandles] {
				eventString, err := pwes.renderer.RenderEvent(pwes.stageContext, fetchedEventHandle, pwes.bookMarkHandle)
				if err != nil {
					log.WithError(err).Errorf("Error Rendering event for event handle %d", fetchedEventHandle)
				} else {
					pwes.eventsQueue.Put(eventString)
				}
			}
		} else {
			if err.(syscall.Errno) == ErrorNoMoreItems {
				log.Infof("No More items in the handle: %d resetting", pwes.signalEventHandle)
				windows.ResetEvent(pwes.signalEventHandle)
				err = nil
			} else if err.(syscall.Errno) == ErrorInvalidHandle {
				log.WithError(err).Warn("Cannot fetch events with this handle")
			} else {
				log.WithError(err).Error("Error fetching event handles")
			}
			break
		}
	}
	return err
}

func (pwes *PullWinEventSubscriber) pollForEventHandles() error {
	//Try fetching first if this fails, try after wait
	err := pwes.fetchEventsImmediately()
	if err == nil && pwes.eventsQueue.Empty() {
		//Wait for system to signal that there are events or timeout
		waitTimeMillis := uint32(pwes.maxWaitTime / time.Millisecond)
		log.Debugf("Waiting %d milliseconds for Events to be notified", waitTimeMillis)
		val, err := windows.WaitForSingleObject(pwes.signalEventHandle, waitTimeMillis)
		waitReturnVal := WaitReturnValue(val)
		switch waitReturnVal {
		case WaitFailed:
			log.WithError(err).Error("Wait Failed")
		case WaitAbandoned:
			log.Info("Wait abandoned")
		case WaitTimeout:
			log.Infof("No Events till the wait, wait time out happened")
		case WaitObject0:
			err = pwes.fetchEventsImmediately()
			if err != nil {
				log.WithError(err).Error("Error fetching event handles")
			}
		default:
			log.Warnf("Unsupported Wait return value : %d", waitReturnVal)
		}
	} else {
		log.WithError(err).Error("Error fetching with subscription handle")
	}
	return err
}

func (pwes *PullWinEventSubscriber) Subscribe() error {
	//Use unique event
	eventUUID := uuid.NewV4()
	n, err := syscall.UTF16PtrFromString(eventUUID.String())
	if err != nil {
		log.WithError(err).Error("Error converting uuid to utf16 ptr :%s", eventUUID.String())
	} else {
		if pwes.signalEventHandle, err = windows.CreateEvent(
			nil,
			1, // Manual Reset - true
			0, //Initial Stage - false (non signalled)
			n,
		); err != nil {
			log.WithError(err).Error("Error Creating Signal Event")
		} else {
			err = pwes.BaseWinEventSubscriber.Subscribe()
		}
	}
	return err
}

func (pwes *PullWinEventSubscriber) Read() ([]api.Record, error) {
	if pwes.eventsQueue.Empty() {
		err := pwes.pollForEventHandles()
		if err != nil {
			log.WithError(err).Error("Error when polling for events")
			return []api.Record{}, err
		}
	}
	return pwes.BaseWinEventSubscriber.Read()
}

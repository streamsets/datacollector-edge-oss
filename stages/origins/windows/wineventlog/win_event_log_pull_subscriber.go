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
	log "github.com/sirupsen/logrus"
	"golang.org/x/sys/windows"
	"syscall"
	"time"
)

type PullWinEventSubscriber struct {
	*BaseWinEventSubscriber
}

func (pwes *PullWinEventSubscriber) fetchEventsImmediately() error {
	fetchedEventHandles := make([]EventHandle, int64(pwes.maxNoOfEvents)-pwes.eventsQueue.Len())
	returnedHandles := uint32(0)
	err := EvtNext(pwes.subscriptionHandle, uint32(len(fetchedEventHandles)), fetchedEventHandles, &returnedHandles)
	if err == nil {
		for _, fetchedEventHandle := range fetchedEventHandles[:returnedHandles] {
			eventString, err := pwes.renderEventXML(fetchedEventHandle)
			if err != nil {
				log.WithError(err).Errorf("Error Rendering XML for event handle %d", fetchedEventHandle)
			} else {
				pwes.eventsQueue.Put(eventString)
			}
		}
	} else if err.(syscall.Errno) == ErrorNoMoreItems || err.(syscall.Errno) == ErrorInvalidHandle {
		if err.(syscall.Errno) == ErrorNoMoreItems {
			log.Infof("No More items in the handle: %d resetting", pwes.signalEventHandle)
			windows.ResetEvent(pwes.signalEventHandle)
		} else {
			log.WithError(err).Warn("Cannot fetch events with this handle")
		}
		err = nil
	}
	return err
}

func (pwes *PullWinEventSubscriber) pollForEventHandles() error {
	//Try fetching first if this fails, try after wait
	err := pwes.fetchEventsImmediately()
	if err == nil && pwes.eventsQueue.Empty() {
		//Wait for system to signal that there are events or timeout
		waitTimeMillis := uint32(pwes.maxWaitTime / time.Millisecond)
		log.Infof("Waiting %d milliseconds for Events to be notified", waitTimeMillis)
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
	}
	return err
}

func (pwes *PullWinEventSubscriber) Subscribe() error {
	n, _ := syscall.UTF16PtrFromString("edge")
	var err error
	if pwes.signalEventHandle, err = windows.CreateEvent(
		nil,
		1, // Manual Reset - true
		0, //Initial Stage - false (non signalled)
		n,
	); err != nil {
		log.WithError(err).Error("Error Creating Signal Event")
		return err
	}

	return pwes.BaseWinEventSubscriber.Subscribe()
}

func (pwes *PullWinEventSubscriber) Read() ([]string, error) {
	if pwes.eventsQueue.Empty() {
		err := pwes.pollForEventHandles()
		if err != nil {
			log.WithError(err).Error("Error when polling for events")
			return []string{}, err
		}
	}
	return pwes.BaseWinEventSubscriber.Read()
}

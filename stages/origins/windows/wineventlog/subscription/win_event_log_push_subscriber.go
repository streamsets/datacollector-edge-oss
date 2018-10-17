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
	log "github.com/sirupsen/logrus"
	wineventsyscall "github.com/streamsets/datacollector-edge/stages/origins/windows/wineventlog/common"
	"syscall"
	"unsafe"
)

type pushWinEventSubscriber struct {
	*baseWinEventSubscriber
}

func (pwes *pushWinEventSubscriber) Subscribe() error {
	pwes.subscriptionCallback = func(
		action wineventsyscall.EvtSubscribeNotifyAction,
		userContext unsafe.Pointer,
		eventHandle wineventsyscall.EventHandle,
	) syscall.Errno {
		var returnStatus syscall.Errno
		switch action {
		case wineventsyscall.EvtSubscribeActionError:
			if wineventsyscall.ErrorEvtQueryResultStale == returnStatus {
				log.Error("The subscription callback was notified that eventHandle records are missing")
			} else {
				log.WithError(syscall.Errno(eventHandle)).Error("The subscription callback received the following Win32 error")
			}
		case wineventsyscall.EvtSubscribeActionDeliver:
			eventRecord, err := pwes.renderer.RenderEvent(pwes.stageContext, eventHandle, pwes.bookMarkHandle)
			if err == nil {
				pwes.eventsQueue.Put(eventRecord)
			} else {
				log.WithError(err).Errorf("Error rendering from event handle %d", eventHandle)
			}
		}
		return returnStatus
	}
	return pwes.baseWinEventSubscriber.Subscribe()
}

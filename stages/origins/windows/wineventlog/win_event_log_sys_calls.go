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
// Copied from https://github.com/streamsets/windataextractor/tree/master/dev/src/lib/win/eventlog
package wineventlog

import (
	//"github.com/clbanning/mxj"
	log "github.com/sirupsen/logrus"
	syswin "golang.org/x/sys/windows"
	"syscall"
	"unsafe"
)

var (
	//Module wevetapi not available in https://github.com/golang/sys/blob/master/windows/zsyscall_windows.go#L488
	winEvtDLL = syscall.NewLazyDLL("wevtapi.dll")

	evtSubscribe = winEvtDLL.NewProc("EvtSubscribe")
	evtRender    = winEvtDLL.NewProc("EvtRender")
	evtClose     = winEvtDLL.NewProc("EvtClose")
	evtNext      = winEvtDLL.NewProc("EvtNext")
)

type EventHandle uintptr
type SubscriptionHandle uintptr
type BookmarkHandle uintptr

//https://docs.microsoft.com/en-us/windows/desktop/api/winevt/ne-winevt-_evt_subscribe_flags
//From winevt.h
//typedef enum _EVT_SUBSCRIBE_FLAGS {
//  EvtSubscribeToFutureEvents        = 1,
//  EvtSubscribeStartAtOldestRecord   = 2,
//  EvtSubscribeStartAfterBookmark    = 3,
//  EvtSubscribeOriginMask            = 0x3,
//  EvtSubscribeTolerateQueryErrors   = 0x1000,
//  EvtSubscribeStrict                = 0x10000
//} EVT_SUBSCRIBE_FLAGS;

type EvtSubscribeFlag uint32

const (
	EvtSubscribeToFutureEvents = EvtSubscribeFlag(iota + 1)
	EvtSubscribeStartAtOldestRecord
	EvtSubscribeStartAfterBookmark
)

//https://docs.microsoft.com/en-us/windows/desktop/api/winevt/ne-winevt-_evt_render_flags
//typedef enum _EVT_RENDER_FLAGS {
//  EvtRenderEventValues   = 0,
//  EvtRenderEventXml      = 1,
//  EvtRenderBookmark      = 2
//} EVT_RENDER_FLAGS;
type EvtRenderFlag uint32

const (
	EvtRenderEventValues = EvtRenderFlag(iota)
	EvtRenderEventXml
	EvtRenderBookmark
)

//https://docs.microsoft.com/en-us/windows/desktop/api/winevt/ne-winevt-_evt_subscribe_notify_action
//typedef enum _EVT_SUBSCRIBE_NOTIFY_ACTION {
//  EvtSubscribeActionError     = 0,
//  EvtSubscribeActionDeliver   = 1
//} EVT_SUBSCRIBE_NOTIFY_ACTION;
type EvtSubscribeNotifyAction uint32

const (
	EvtSubscribeActionError  = EvtSubscribeNotifyAction(iota)
	EvtSubscribeActionDeliver
)

//https://docs.microsoft.com/en-us/windows/desktop/api/winevt/ne-winevt-_evt_format_message_flags
//TODO

//https://docs.microsoft.com/en-us/windows/desktop/debug/system-error-codes--0-499-
//https://docs.microsoft.com/en-us/windows/desktop/wes/windows-event-log-error-constants
const (
	ErrorInvalidHandle = syscall.Errno(6)

	ErrorInsufficientBuffer = syscall.Errno(0x7A)

	ErrorNoMoreItems = syscall.Errno(259) //(0x103)


	ErrorInvalidQuery        = syscall.Errno(15001)
	ErrorEvtQueryResultStale = syscall.Errno(15011)
	ErrorEvtChannelNotFound  = syscall.Errno(15007)
)

//https://docs.microsoft.com/en-us/windows/desktop/WinProg/windows-data-types

//https://msdn.microsoft.com/en-us/935a787c-fd71-492d-a803-80cb2c9019ea
//typedef DWORD ( WINAPI *EvtSubscribeCallback)(
//   EvtSubscribeNotifyAction Action,
//   PVOID                       UserContext,
//   EventHandle                  Event
//);
//PVOID pointer to any
type EvtSubscribeCallback func(
	Action EvtSubscribeNotifyAction,
	UserContext unsafe.Pointer,
	Event EventHandle,
) syscall.Errno

//------------------------------------- SYSCALL Helpers -------------------------------------------------------------

func convertStringToUtf16ToUintPtr(str string) (uintptr, error) {
	ptr := uintptr(0)
	var err error
	if str != "" {
		var uint16ptr *uint16
		uint16ptr, err = syscall.UTF16PtrFromString(str)
		if err == nil {
			ptr = uintptr(unsafe.Pointer(uint16ptr))
		}
	}
	return ptr, err
}

func processSysCallReturn(r1 uintptr, e1 error) error {
	var err error
	if r1 & 0xff == 0 {
		if e1.(syscall.Errno) != 0 {
			err = e1
		} else {
			err = syscall.EINVAL
		}
	}
	return err
}

func EvtSubscribe(
	signalEventHandle syswin.Handle,
	channelPath string,
	query string,
	bookmark BookmarkHandle,
	callback EvtSubscribeCallback,
	flags EvtSubscribeFlag,
) (SubscriptionHandle, error) {
	log.Debug("EvtSubscribe called")
	var err error
	var cpPtr, queryPtr, callbackPtr uintptr

	if cpPtr, err = convertStringToUtf16ToUintPtr(channelPath); err != nil {
		return 0, err
	}

	if queryPtr, err = convertStringToUtf16ToUintPtr(query); err != nil {
		return 0, err
	}

	if callback != nil {
		callbackPtr = syscall.NewCallback(callback)
	}

	//EventHandle EvtSubscribe(
	//  EventHandle             Session,
	//  HANDLE                 SignalEvent,
	//  LPCWSTR                ChannelPath,
	//  LPCWSTR                Query,
	//  EventHandle             Bookmark,
	//  PVOID                  Context,
	//  EvtSubscribeCallback Callback,
	//  DWORD                  Flags
	//);
	//DWORD a dword is an unsigned, 32-bit unit of data
	//PDWORD *DWORD //A pointer to a DWORD.
	//PVOID pointer to any
	//LPWSTR pointer to a null-terminated string of 16-bit Unicode characters
	r1, _, e1 := evtSubscribe.Call(
		uintptr(0),
		uintptr(signalEventHandle),
		cpPtr,
		queryPtr,
		uintptr(bookmark),
		uintptr(0),
		callbackPtr,
		uintptr(flags),
	)

	err = processSysCallReturn(r1, e1)
	return SubscriptionHandle(r1), err
}

func EvtRender(
	context EventHandle,
	fragment EventHandle,
	flags EvtRenderFlag,
	bufferSize uint32,
	bufferPtr unsafe.Pointer,
	bufferUsedPtr *uint32,
	PropertyCountPtr *uint32,
) error {
	log.Debug("EvtRender Called")

	//BOOL EvtRender(
	//  EventHandle Context,
	//  EventHandle Fragment,
	//  DWORD      Flags,
	//  DWORD      BufferSize,
	//  PVOID      Buffer,
	//  PDWORD     BufferUsed,
	//  PDWORD     PropertyCount
	//);
	//DWORD a dword is an unsigned, 32-bit unit of data
	//PDWORD *DWORD //A pointer to a DWORD.
	//PVOID pointer to any
	r1, _, e1 := evtRender.Call(
		uintptr(context),
		uintptr(fragment),
		uintptr(flags),
		uintptr(bufferSize),
		uintptr(bufferPtr),
		uintptr(unsafe.Pointer(bufferUsedPtr)),
		uintptr(unsafe.Pointer(PropertyCountPtr)),
	)
	return processSysCallReturn(r1, e1)
}

func EvtClose(handle uintptr) {
	log.Debug("EvtClose Called")
	////BOOL EvtClose(
	////  EVT_HANDLE Object
	////);
	//
	r1, _, e1 := evtClose.Call(handle)
	err := processSysCallReturn(r1, e1)
	if err != nil {
		log.WithError(err).Warn("Error Closing event handle")
	}
}

func EvtNext(resultSet SubscriptionHandle, eventsSize uint32, events []EventHandle, returnedHandles *uint32) error {
	log.Debug("EvtNext Called")
	waitTime := uint32(0)
	flags := 0
	//BOOL EvtNext(
	//  EVT_HANDLE  ResultSet,
	//  DWORD       EventsSize,
	//  PEVT_HANDLE Events,
	//  DWORD       Timeout,
	//  DWORD       Flags,
	//  PDWORD      Returned
	//);
	r1, _, e1 := evtNext.Call(
		uintptr(resultSet),
		uintptr(eventsSize),
		uintptr(unsafe.Pointer(&events[0])),
		uintptr(waitTime),
		uintptr(flags),
		uintptr(unsafe.Pointer(returnedHandles)),
	)
	return processSysCallReturn(r1, e1)
}


//https://docs.microsoft.com/en-us/windows/desktop/api/synchapi/nf-synchapi-waitforsingleobject
type WaitReturnValue uint32
const (
	WaitObject0   = WaitReturnValue(0x00000000)
	WaitAbandoned = WaitReturnValue(0x00000080)
	WaitTimeout   = WaitReturnValue(0x00000102)
	WaitFailed    = WaitReturnValue(0xFFFFFFFF)
)
//------------------------------------------------------------------------------------------------------------

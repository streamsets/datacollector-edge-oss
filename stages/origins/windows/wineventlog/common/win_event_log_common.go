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
package common

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/go-ole/go-ole"
	log "github.com/sirupsen/logrus"
	wincommon "github.com/streamsets/datacollector-edge/stages/origins/windows/common"
	syswin "golang.org/x/sys/windows"
	"syscall"
	"time"
	"unsafe"
)

var (
	//Module wevetapi not available in https://github.com/golang/sys/blob/master/windows/zsyscall_windows.go#L488
	winEvtDLL = syscall.NewLazyDLL("wevtapi.dll")

	evtSubscribe      = winEvtDLL.NewProc("EvtSubscribe")
	evtRender         = winEvtDLL.NewProc("EvtRender")
	evtClose          = winEvtDLL.NewProc("EvtClose")
	evtNext           = winEvtDLL.NewProc("EvtNext")
	evtCreateBookmark = winEvtDLL.NewProc("EvtCreateBookmark")
	evtUpdateBookmark = winEvtDLL.NewProc("EvtUpdateBookmark")
	//evtOpenPublisherEnum = winEvtDLL.NewProc("EvtOpenPublisherEnum")
	//evtNextPublisherId = winEvtDLL.NewProc("EvtNextPublisherId")
	evtOpenPublisherMetadata = winEvtDLL.NewProc("EvtOpenPublisherMetadata")
	evtFormatMessage         = winEvtDLL.NewProc("EvtFormatMessage")
	evtCreateRenderContext   = winEvtDLL.NewProc("EvtCreateRenderContext")
)

type EventHandle uintptr
type SubscriptionHandle uintptr
type BookmarkHandle uintptr
type EventRenderContextHandle uintptr
type PublisherMetadataHandle uintptr

func (e EventHandle) Close() {
	EvtClose(uintptr(e))
}

func (sh SubscriptionHandle) Close() {
	EvtClose(uintptr(sh))
}

func (bh BookmarkHandle) Close() {
	EvtClose(uintptr(bh))
}

func (erch EventRenderContextHandle) Close() {
	EvtClose(uintptr(erch))
}
func (pmh PublisherMetadataHandle) Close() {
	EvtClose(uintptr(pmh))
}

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
	EvtSubscribeActionError = EvtSubscribeNotifyAction(iota)
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
	if r1&0xff == 0 {
		if e1.(syscall.Errno) != 0 {
			err = e1
		} else {
			err = syscall.EINVAL
		}
	}
	return err
}

//https://docs.microsoft.com/en-us/windows/desktop/api/winevt/nf-winevt-evtsubscribe
func EvtSubscribe(
	signalEventHandle syswin.Handle,
	channelPath string,
	query string,
	bookmark BookmarkHandle,
	callback EvtSubscribeCallback,
	flags EvtSubscribeFlag,
) (SubscriptionHandle, error) {
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

//https://docs.microsoft.com/en-us/windows/desktop/api/winevt/nf-winevt-evtrender
func EvtRender(
	context EventRenderContextHandle,
	fragment EventHandle,
	flags EvtRenderFlag,
	bufferSize uint32,
	bufferPtr *byte,
	bufferUsedPtr *uint32,
	PropertyCountPtr *uint32,
) error {
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
		uintptr(unsafe.Pointer(bufferPtr)),
		uintptr(unsafe.Pointer(bufferUsedPtr)),
		uintptr(unsafe.Pointer(PropertyCountPtr)),
	)
	return processSysCallReturn(r1, e1)
}

//https://docs.microsoft.com/en-us/windows/desktop/api/winevt/nf-winevt-evtclose
func EvtClose(handle uintptr) {
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

//https://docs.microsoft.com/en-us/windows/desktop/api/winevt/nf-winevt-evtnext
func EvtNext(resultSet SubscriptionHandle, eventsSize uint32, events []EventHandle, returnedHandles *uint32) error {
	//Don't wait in EvtNext
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

//https://docs.microsoft.com/en-us/windows/desktop/api/winevt/nf-winevt-evtcreatebookmark
func EvtCreateBookmark(bookmarkXML string) (BookmarkHandle, error) {
	var bookmarkXMLPtr uintptr
	var err error
	var bookMarkHandle = BookmarkHandle(0)
	if bookmarkXMLPtr, err = convertStringToUtf16ToUintPtr(bookmarkXML); err == nil {
		//EVT_HANDLE EvtCreateBookmark(
		//  LPCWSTR BookmarkXml
		//);
		//LPWSTR pointer to a null-terminated string of 16-bit Unicode characters
		r1, _, e1 := evtCreateBookmark.Call(bookmarkXMLPtr)
		err = processSysCallReturn(r1, e1)
		if err == nil {
			bookMarkHandle = BookmarkHandle(r1)
		}
	}
	return bookMarkHandle, err
}

//https://docs.microsoft.com/en-us/windows/desktop/api/winevt/nf-winevt-evtupdatebookmark
func EvtUpdateBookmark(bookmarkHandle BookmarkHandle, eventHandle EventHandle) error {
	//BOOL EvtUpdateBookmark(
	//  EVT_HANDLE Bookmark,
	//  EVT_HANDLE Event
	//);
	r1, _, e1 := evtUpdateBookmark.Call(uintptr(bookmarkHandle), uintptr(eventHandle))
	return processSysCallReturn(r1, e1)
}

//https://docs.microsoft.com/en-us/windows/desktop/wes/formatting-event-messages

//https://docs.microsoft.com/en-us/windows/desktop/api/winevt/nf-winevt-evtopenpublishermetadata
func EvtOpenPublisherMetadata(publisherId string) (PublisherMetadataHandle, error) {
	publisherIdPtr, err := convertStringToUtf16ToUintPtr(publisherId)
	publisherMetadataHandle := PublisherMetadataHandle(0)
	if err == nil {
		//EVT_HANDLE EvtOpenPublisherMetadata(
		//  EVT_HANDLE Session,
		//  LPCWSTR    PublisherId,
		//  LPCWSTR    LogFilePath,
		//  LCID       Locale,
		//  DWORD      Flags
		//);
		r1, _, e1 := evtOpenPublisherMetadata.Call(0, publisherIdPtr, 0, 0, 0)
		err = processSysCallReturn(r1, e1)
		publisherMetadataHandle = PublisherMetadataHandle(r1)
	}
	return publisherMetadataHandle, err
}

//typedef enum _EVT_FORMAT_MESSAGE_FLAGS {
//  EvtFormatMessageEvent      = 1,
//  EvtFormatMessageLevel      = 2,
//  EvtFormatMessageTask       = 3,
//  EvtFormatMessageOpcode     = 4,
//  EvtFormatMessageKeyword    = 5,
//  EvtFormatMessageChannel    = 6,
//  EvtFormatMessageProvider   = 7,
//  EvtFormatMessageId         = 8,
//  EvtFormatMessageXml        = 9
//} EVT_FORMAT_MESSAGE_FLAGS;

type EvtFormatMessageFlag uint32

const (
	EvtFormatMessageEvent = EvtFormatMessageFlag(iota + 1)
	EvtFormatMessageLevel
	EvtFormatMessageTask
	EvtFormatMessageOpcode
	EvtFormatMessageKeyword
	EvtFormatMessageChannel
	EvtFormatMessageProvider
	EvtFormatMessageId
	EvtFormatMessageXml
)

// https://docs.microsoft.com/en-us/windows/desktop/api/winevt/nf-winevt-evtformatmessage
func EvtFormatMessage(
	publisherMetadataHandle PublisherMetadataHandle,
	eventHandle EventHandle,
	flags EvtFormatMessageFlag,
	bufferSize uint32,
	bufferPtr *byte,
	bufferUsed *uint32,
) error {
	//BOOL EvtFormatMessage(
	//  EVT_HANDLE   PublisherMetadata,
	//  EVT_HANDLE   Event,
	//  DWORD        MessageId,
	//  DWORD        ValueCount,
	//  PEVT_VARIANT Values,
	//  DWORD        Flags,
	//  DWORD        BufferSize,
	//  LPWSTR       Buffer,
	//  PDWORD       BufferUsed
	//);

	r1, _, e1 := evtFormatMessage.Call(
		uintptr(publisherMetadataHandle),
		uintptr(eventHandle),
		0,
		0,
		0,
		uintptr(flags),
		uintptr(bufferSize),
		uintptr(unsafe.Pointer(bufferPtr)),
		uintptr(unsafe.Pointer(bufferUsed)),
	)
	return processSysCallReturn(r1, e1)
}

//typedef enum _EVT_RENDER_CONTEXT_FLAGS {
//  EvtRenderContextValues   = 0,
//  EvtRenderContextSystem   = 1,
//  EvtRenderContextUser     = 2
//} EVT_RENDER_CONTEXT_FLAGS;
type EvtRenderContextFlag uint32

const (
	EvtRenderContextValues = EvtRenderContextFlag(iota)
	EvtRenderContextSystem
	EvtRenderContextUser
)

func EvtCreateRenderContext(flags EvtRenderContextFlag) (EventRenderContextHandle, error) {
	//EVT_HANDLE EvtCreateRenderContext(
	//  DWORD   ValuePathsCount,
	//  LPCWSTR *ValuePaths,
	//  DWORD   Flags
	//);
	eventRenderContextHandle := EventRenderContextHandle(0)
	r1, _, e1 := evtCreateRenderContext.Call(0, 0, uintptr(flags))
	err := processSysCallReturn(r1, e1)
	if err == nil {
		eventRenderContextHandle = EventRenderContextHandle(r1)
	}
	return eventRenderContextHandle, err
}

//typedef enum _EVT_SYSTEM_PROPERTY_ID {
//  EvtSystemProviderName        = 0,
//  EvtSystemProviderGuid,
//  EvtSystemEventID,
//  EvtSystemQualifiers,
//  EvtSystemLevel,
//  EvtSystemTask,
//  EvtSystemOpcode,
//  EvtSystemKeywords,
//  EvtSystemTimeCreated,
//  EvtSystemEventRecordId,
//  EvtSystemActivityID,
//  EvtSystemRelatedActivityID,
//  EvtSystemProcessID,
//  EvtSystemThreadID,
//  EvtSystemChannel,
//  EvtSystemComputer,
//  EvtSystemUserID,
//  EvtSystemVersion,
//  EvtSystemPropertyIdEND
//} EVT_SYSTEM_PROPERTY_ID;

type EvtSystemPropertyId uint32

const (
	EvtSystemProviderName = EvtSystemPropertyId(iota)
	EvtSystemProviderGuid
	EvtSystemEventID
	EvtSystemQualifiers
	EvtSystemLevel
	EvtSystemTask
	EvtSystemOpcode
	EvtSystemKeywords
	EvtSystemTimeCreated
	EvtSystemEventRecordId
	EvtSystemActivityID
	EvtSystemRelatedActivityID
	EvtSystemProcessID
	EvtSystemThreadID
	EvtSystemChannel
	EvtSystemComputer
	EvtSystemUserID
	EvtSystemVersion
	EvtSystemPropertyIdEND
)

var (
	SystemPropertyIds = []string{
		"Provider Name",
		"Provider Guid",
		"Event Id",
		"Qualifiers",
		"Level",
		"Task",
		"Opcode",
		"Keywords",
		"TimeCreated",
		"EventRecordId",
		"ActivityId",
		"RelatedActivityId",
		"ProcessId",
		"ThreadId",
		"Channel",
		"Computer",
		"UserId",
		"Version",
	}
)

//typedef enum _EVT_VARIANT_TYPE {
//  EvtVarTypeNull         = 0,
//  EvtVarTypeString       = 1,
//  EvtVarTypeAnsiString   = 2,
//  EvtVarTypeSByte        = 3,
//  EvtVarTypeByte         = 4,
//  EvtVarTypeInt16        = 5,
//  EvtVarTypeUInt16       = 6,
//  EvtVarTypeInt32        = 7,
//  EvtVarTypeUInt32       = 8,
//  EvtVarTypeInt64        = 9,
//  EvtVarTypeUInt64       = 10,
//  EvtVarTypeSingle       = 11,
//  EvtVarTypeDouble       = 12,
//  EvtVarTypeBoolean      = 13,
//  EvtVarTypeBinary       = 14,
//  EvtVarTypeGuid         = 15,
//  EvtVarTypeSizeT        = 16,
//  EvtVarTypeFileTime     = 17,
//  EvtVarTypeSysTime      = 18,
//  EvtVarTypeSid          = 19,
//  EvtVarTypeHexInt32     = 20,
//  EvtVarTypeHexInt64     = 21,
//  EvtVarTypeEvtHandle    = 32,
//  EvtVarTypeEvtXml       = 35
//} EVT_VARIANT_TYPE;
type EvtVariantType uint32

const (
	EvtVarTypeNull = EvtVariantType(iota)
	EvtVarTypeString
	EvtVarTypeAnsiString
	EvtVarTypeSByte
	EvtVarTypeByte
	EvtVarTypeInt16
	EvtVarTypeUInt16
	EvtVarTypeInt32
	EvtVarTypeUInt32
	EvtVarTypeInt64
	EvtVarTypeUInt64
	EvtVarTypeSingle
	EvtVarTypeDouble
	EvtVarTypeBoolean
	EvtVarTypeBinary
	EvtVarTypeGuid
	EvtVarTypeSizeT
	EvtVarTypeFileTime
	EvtVarTypeSysTime
	EvtVarTypeSid
	EvtVarTypeHexInt32
	EvtVarTypeHexInt64
	EvtVarTypeEvtHandle = EvtVariantType(32)
	EvtVarTypeEvtXml    = EvtVariantType(35)
)

//typedef struct _EVT_VARIANT {
//  union {
//    BOOL       BooleanVal;
//    INT8       SByteVal;
//    INT16      Int16Val;
//    INT32      Int32Val;
//    INT64      Int64Val;
//    UINT8      ByteVal;
//    UINT16     UInt16Val;
//    UINT32     UInt32Val;
//    UINT64     UInt64Val;
//    float      SingleVal;
//    double     DoubleVal;
//    ULONGLONG  FileTimeVal;
//    SYSTEMTIME *SysTimeVal;
//    GUID       *GuidVal;
//    LPCWSTR    StringVal;
//    LPCSTR     AnsiStringVal;
//    PBYTE      BinaryVal;
//    PSID       SidVal;
//    size_t     SizeTVal;
//    BOOL       *BooleanArr;
//    INT8       *SByteArr;
//    INT16      *Int16Arr;
//    INT32      *Int32Arr;
//    INT64      *Int64Arr;
//    UINT8      *ByteArr;
//    UINT16     *UInt16Arr;
//    UINT32     *UInt32Arr;
//    UINT64     *UInt64Arr;
//    float      *SingleArr;
//    double     *DoubleArr;
//    FILETIME   *FileTimeArr;
//    SYSTEMTIME *SysTimeArr;
//    GUID       *GuidArr;
//    LPWSTR     *StringArr;
//    LPSTR      *AnsiStringArr;
//    PSID       *SidArr;
//    size_t     *SizeTArr;
//    EVT_HANDLE EvtHandleVal;
//    LPCWSTR    XmlVal;
//    LPCWSTR    *XmlValArr;
//  };
//  DWORD Count;
//  DWORD Type;
//} EVT_VARIANT, *PEVT_VARIANT;

//void printSize(){
//     printf("Size:%d\n", sizeof(struct __EVT_VARIANT));
//}
//
// The above C code returns size has 16 bytes, So:
// Union - (16 - (4 + 4)) = 8 bytes
// Count - DWORD(uint32) - 32 bits - 4 bytes
// Type  - DWORD(uint32) - 32 bits - 4 bytes

type EvtVariant struct {
	data           [8]byte //8 bytes
	count          uint32
	evtVariantType EvtVariantType
}

func (evtVariant *EvtVariant) GetData() interface{} {
	buf := evtVariant.data
	var err error
	var returnVal interface{}
	switch evtVariant.evtVariantType {
	case EvtVarTypeNull:
		return nil
	case EvtVarTypeByte:
		return evtVariant.data[0]
	case EvtVarTypeBoolean:
		boolVal := *(*bool)(unsafe.Pointer(&buf[0]))
		if err == nil {
			returnVal = boolVal
		}
	case EvtVarTypeUInt16:
		returnVal = binary.LittleEndian.Uint16(buf[:])
	case EvtVarTypeUInt32:
		returnVal = binary.LittleEndian.Uint32(buf[:])
	case EvtVarTypeUInt64:
		returnVal = binary.LittleEndian.Uint64(buf[:])
	case EvtVarTypeInt16:
		returnVal = *(*int16)(unsafe.Pointer(&buf[0]))
	case EvtVarTypeInt32:
		returnVal = *(*int32)(unsafe.Pointer(&buf[0]))
	case EvtVarTypeInt64:
		returnVal = *(*int64)(unsafe.Pointer(&buf[0]))
	case EvtVarTypeHexInt32:
		uint32Val := binary.LittleEndian.Uint32(buf[:])
		//Formatting as a hex string for display
		returnVal = fmt.Sprintf("%#x", uint32Val)
	case EvtVarTypeHexInt64:
		uint64Val := binary.LittleEndian.Uint64(buf[:])
		//Formatting as a hex string for display
		returnVal = fmt.Sprintf("%#x", uint64Val)
	case EvtVarTypeFileTime:
		fileTimeVal := *(*syswin.Filetime)(unsafe.Pointer(&buf[0]))
		returnVal = time.Unix(0, fileTimeVal.Nanoseconds())
	case EvtVarTypeSysTime:
		st := *(*syswin.Systemtime)(unsafe.Pointer(&buf[0]))
		returnVal = time.Date(
			int(st.Year),
			time.Month(int(st.Month)),
			int(st.Day),
			int(st.Hour),
			int(st.Minute),
			int(st.Second),
			int(st.Milliseconds*1000),
			time.Now().Location(),
		)
	case EvtVarTypeSingle:
		float32Val := *(*float32)(unsafe.Pointer(&buf[0]))
		if err == nil {
			returnVal = float32Val
		}
	case EvtVarTypeDouble:
		float64Val := *(*float64)(unsafe.Pointer(&buf[0]))
		if err == nil {
			returnVal = float64Val
		}
	case EvtVarTypeString:
		//LPCWSTR - ptr 2 bytes wide string
		ptr := uintptr(binary.LittleEndian.Uint64(buf[:]))
		byteIncrement := unsafe.Sizeof(byte(0))
		byteData := make([]byte, int(evtVariant.count*2))
		//Count * 2 because -> count will represent 2 bytes (as it is uint16)
		for i := 0; i < int(evtVariant.count*2); i++ {
			byteAddress := (*byte)(unsafe.Pointer(ptr))
			byteData[i] = *byteAddress
			ptr = ptr + byteIncrement
		}
		returnVal, err = wincommon.ExtractString(byteData)
	case EvtVarTypeBinary:
		ptr := uintptr(binary.LittleEndian.Uint64(buf[:]))
		byteIncrement := unsafe.Sizeof(byte(0))
		byteData := make([]byte, int(evtVariant.count))
		for i := 0; i < int(evtVariant.count); i++ {
			byteAddress := (*byte)(unsafe.Pointer(ptr))
			byteData[i] = *byteAddress
			ptr = ptr + byteIncrement
		}
		returnVal = byteData
	case EvtVarTypeSid:
		ptr := uintptr(binary.LittleEndian.Uint64(buf[:]))
		sidPtr := (*syswin.SID)(unsafe.Pointer(ptr))
		var sidInfo *wincommon.SIDInfo
		sidInfo, err = wincommon.GetSidInfo(sidPtr)
		if err == nil {
			returnVal = map[string]interface{}{
				"username": sidInfo.Name,
				"domain":   sidInfo.Domain,
				"sidType": map[string]interface{}{
					"Type":         uint32(sidInfo.SIDType),
					"MappedString": sidInfo.SIDType.GetSidTypeString(),
				},
			}
		} else {
			previousError := err
			returnVal = sidPtr.String()
			if err != nil {
				log.WithError(err).Error("Error decoding SID")
				returnVal = nil
			} else {
				log.WithError(previousError).Warn("Error looking up SID info")
			}
		}
	case EvtVarTypeGuid:
		ptr := uintptr(binary.LittleEndian.Uint64(buf[:]))
		gidPtr := (*ole.GUID)(unsafe.Pointer(ptr))
		returnVal = gidPtr.String()
	default:
		err = errors.New("unsupported data type")
		returnVal = buf[:]
	}

	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"type":     evtVariant.evtVariantType,
			"count":    evtVariant.count,
			"byteData": evtVariant.data,
		}).Warn("Error decoding data")
	}
	return returnVal
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

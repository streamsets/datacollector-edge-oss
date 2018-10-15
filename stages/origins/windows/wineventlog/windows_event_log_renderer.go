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
	"strconv"
	"time"
	"unsafe"
)

const (
	BufferSizeDefault = uint32(8 * 1024)
)

var (
	SizeOfEvtVariant = int(unsafe.Sizeof(EvtVariant{}))
)

func ConvertTimeToLong(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond) / int64(time.Nanosecond)
}

type WinEventLogRenderer struct {
	bufferSize                   int
	bufferForRender              []byte
	publisherManager             *WinEventLogPublisherManager
	evtCreateRenderContextHandle EventRenderContextHandle
}

func NewWinEventLogRenderer(bufferSize int) *WinEventLogRenderer {
	bufferSizeValue := BufferSizeDefault
	if bufferSize != -1 {
		bufferSizeValue = uint32(bufferSize)
	}
	publisherManager := &WinEventLogPublisherManager{
		providerToPublisherMetadataHandle: map[string]PublisherMetadataHandle{},
	}
	return &WinEventLogRenderer{
		bufferSize:       bufferSize,
		bufferForRender:  make([]byte, bufferSizeValue),
		publisherManager: publisherManager,
	}
}

func (weler *WinEventLogRenderer) RenderBookmark(bookmarkHandle BookmarkHandle) (string, error) {
	var bookmark string
	returnBytes, _, _, err := weler.render(
		EventHandle(uintptr(bookmarkHandle)),
		EventRenderContextHandle(0),
		EvtRenderBookmark,
	)
	if err == nil {
		bookmark, err = ExtractString(returnBytes[:])
	}
	return bookmark, err
}

func (weler *WinEventLogRenderer) RenderEvent(
	stageContext api.StageContext,
	eventHandle EventHandle,
	bookmarkHandle BookmarkHandle,
) (api.Record, error) {
	var err error
	var record api.Record
	if weler.evtCreateRenderContextHandle == 0 {
		weler.evtCreateRenderContextHandle, err = EvtCreateRenderContext(EvtRenderContextSystem)
		if err != nil {
			log.WithError(err).Errorf("Error creating rendering context for system")
		}
	}
	if err == nil {
		var eventXMLString, recordIdString string
		var systemData interface{}
		eventField := make(map[string]interface{})
		eventXMLString, err = weler.renderEventXML(eventHandle)
		if err == nil {
			eventField["rawEventXML"] = eventXMLString
			systemData, err = weler.renderSystemData(eventHandle)
			if err == nil {
				eventField["System"] = systemData
				if systemData != nil {
					providerName := systemData.(map[string]interface{})[SystemPropertyIds[EvtSystemProviderName]].(string)
					recordId := systemData.(map[string]interface{})[SystemPropertyIds[EvtSystemEventRecordId]].(uint64)
					computerName := systemData.(map[string]interface{})[SystemPropertyIds[EvtSystemComputer]].(string)
					channel := systemData.(map[string]interface{})[SystemPropertyIds[EvtSystemChannel]].(string)
					timeCreated := systemData.(map[string]interface{})[SystemPropertyIds[EvtSystemTimeCreated]].(time.Time)

					eventField["Message"], err = weler.renderMessageStrings(eventHandle, providerName)
					if err != nil {
						log.WithError(err).Warn("Error rendering message strings")
						err = nil
					}
					recordIdString = computerName + "::" + channel + "::" +
						strconv.FormatUint(recordId, 10) + "::" +
						strconv.FormatInt(ConvertTimeToLong(timeCreated), 10)
				}
			}
			if recordIdString == "" {
				recordIdString = uuid.NewV4().String()
			}
			record, err = stageContext.CreateRecord(recordIdString, eventField)
			if err != nil {
				log.WithError(err).Error("Error creating record")
			}
			if err == nil {
				EvtUpdateBookmark(bookmarkHandle, eventHandle)
			}
		}
	}
	return record, err
}

func (weler *WinEventLogRenderer) Close() {
	if weler.publisherManager != nil {
		weler.publisherManager.Close()
	}
	if weler.evtCreateRenderContextHandle != 0 {
		weler.evtCreateRenderContextHandle.Close()
	}
}

//private methods
func (weler *WinEventLogRenderer) renderMessageStrings(eventHandle EventHandle, provider string) (string, error) {
	var message string
	publisherHandle, err := weler.publisherManager.GetPublisherHandle(provider)
	if err == nil {
		dwBufferUsed := uint32(0)
		if err = EvtFormatMessage(
			publisherHandle,
			eventHandle,
			EvtFormatMessageEvent,
			uint32(len(weler.bufferForRender)),
			&weler.bufferForRender[0],
			&dwBufferUsed,
		); err != nil {
			if err == ErrorInsufficientBuffer && weler.bufferSize == -1 {
				log.Debugf(
					"Insufficient Buffer for rendering with length: %d. Retrying with Buffer of size: %d",
					len(weler.bufferForRender),
					dwBufferUsed,
				)
				weler.bufferForRender = make([]byte, dwBufferUsed)
				err = EvtFormatMessage(
					publisherHandle,
					eventHandle,
					EvtFormatMessageEvent,
					uint32(len(weler.bufferForRender)),
					&weler.bufferForRender[0],
					&dwBufferUsed,
				)
			} else {
				log.WithError(err).Error("Error formatting message strings")
			}
		}
		if err == nil {
			message, err = ExtractString(weler.bufferForRender[:(dwBufferUsed * 2)])
		}
	}
	return message, err
}

func (weler *WinEventLogRenderer) renderSystemData(eventHandle EventHandle) (interface{}, error) {
	systemProperties := make(map[string]interface{})
	var err error
	if err == nil {
		var buf []byte
		var propertyCount uint32
		buf, _, propertyCount, err = weler.render(eventHandle, weler.evtCreateRenderContextHandle, EvtRenderEventValues)
		if err == nil {
			propertyId := 0
			start := 0
			end := SizeOfEvtVariant
			for propertyId < int(propertyCount) {
				bufSlice := buf[start:end]
				evtVariant := (*EvtVariant)(unsafe.Pointer(&bufSlice[0]))
				data := evtVariant.GetData()
				if data != nil {
					systemProperties[SystemPropertyIds[propertyId]] = data
				}
				start = end
				end += SizeOfEvtVariant
				propertyId += 1
			}
		} else {
		}
	} else {
		log.WithError(err).Error("Error rendering system data")
	}
	return systemProperties, err
}

func (weler *WinEventLogRenderer) renderEventXML(eventHandle EventHandle) (string, error) {
	var err error
	var buf []byte
	var eventXmlString string
	buf, _, _, err = weler.render(eventHandle, 0, EvtRenderEventXml)
	if err == nil {
		eventXmlString, err = ExtractString(buf)
	}
	return eventXmlString, err
}

func (weler *WinEventLogRenderer) render(
	eventHandle EventHandle,
	eventRenderContextHandle EventRenderContextHandle,
	flags EvtRenderFlag,
) ([]byte, uint32, uint32, error) {
	dwBufferUsed := uint32(0)
	dwPropertyCount := uint32(0)
	err := EvtRender(
		eventRenderContextHandle,
		eventHandle,
		flags,
		uint32(len(weler.bufferForRender)),
		&weler.bufferForRender[0],
		&dwBufferUsed,
		&dwPropertyCount,
	)
	if err != nil && err == ErrorInsufficientBuffer && weler.bufferSize == -1 {
		log.Debugf(
			"Insufficient Buffer for rendering with length: %d. Retrying with Buffer of size: %d",
			len(weler.bufferForRender),
			dwBufferUsed,
		)
		weler.bufferForRender = make([]byte, dwBufferUsed)
		err = EvtRender(
			eventRenderContextHandle,
			eventHandle,
			flags,
			uint32(len(weler.bufferForRender)),
			&weler.bufferForRender[0],
			&dwBufferUsed,
			&dwPropertyCount,
		)
	}
	if err != nil {
		log.WithError(err).Errorf("Render Failed")
	}
	return weler.bufferForRender, dwBufferUsed, dwPropertyCount, err
}

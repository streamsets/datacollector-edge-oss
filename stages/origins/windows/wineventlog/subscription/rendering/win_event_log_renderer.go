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
package rendering

import (
	"github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cast"
	"github.com/streamsets/datacollector-edge/api"
	wincommon "github.com/streamsets/datacollector-edge/stages/origins/windows/common"
	winevtcommon "github.com/streamsets/datacollector-edge/stages/origins/windows/wineventlog/common"
	"strconv"
	"unsafe"
)

type RawEventPopulationStrategy string

const (
	BufferSizeDefault          = uint32(8 * 1024)
	PopulateRawEventXMLAlways  = "ALWAYS"
	PopulateRawEventXMLOnError = "ON_ERROR"
)

var (
	SizeOfEvtVariant = int(unsafe.Sizeof(winevtcommon.EvtVariant{}))
)

type WinEventLogRenderer struct {
	bufferSize                   int
	rawEventPopulationStrategy   RawEventPopulationStrategy
	bufferForRender              []byte
	publisherManager             *winEventLogPublisherManager
	evtCreateRenderContextHandle winevtcommon.EventRenderContextHandle
}

func (weler *WinEventLogRenderer) RenderBookmark(bookmarkHandle winevtcommon.BookmarkHandle) (string, error) {
	var bookmark string
	returnBytes, _, _, err := weler.render(
		winevtcommon.EventHandle(uintptr(bookmarkHandle)),
		winevtcommon.EventRenderContextHandle(0),
		winevtcommon.EvtRenderBookmark,
	)
	if err == nil {
		bookmark, err = wincommon.ExtractString(returnBytes[:])
	}
	return bookmark, err
}

func (weler *WinEventLogRenderer) RenderEvent(
	stageContext api.StageContext,
	eventHandle winevtcommon.EventHandle,
	bookmarkHandle winevtcommon.BookmarkHandle,
) (api.Record, error) {
	var err error
	var record api.Record
	if weler.evtCreateRenderContextHandle == 0 {
		weler.evtCreateRenderContextHandle, err =
			winevtcommon.EvtCreateRenderContext(winevtcommon.EvtRenderContextSystem)
		if err != nil {
			log.WithError(err).Errorf("Error creating rendering context for system")
		}
	}
	if err == nil {
		var recordIdString string
		populateRawEventXML := weler.rawEventPopulationStrategy == PopulateRawEventXMLAlways
		log.Debugf("Populating Raw Event XML : %s %v", weler.rawEventPopulationStrategy, populateRawEventXML)
		var systemData interface{}
		eventField := make(map[string]interface{})
		if err == nil {
			systemData, err = weler.renderSystemData(eventHandle)
			if err == nil {
				eventField["System"] = systemData
				if systemData != nil {
					var providerName string
					recordIdString, providerName, err = getRecordIdAndProviderName(systemData)
					if err == nil {
						eventField["Message"], err = weler.renderMessageStrings(eventHandle, providerName)
						if err != nil {
							log.WithError(err).Warn("Error rendering message strings")
							populateRawEventXML = true
							err = nil
						}
					}
				}
			}

			//Populate the raw event xml field if the populate raw event xml flag was set or if there was an error
			log.Debugf("Populating Raw Event XML : %v %v", populateRawEventXML, err != nil)

			if populateRawEventXML || err != nil {
				eventXMLString, err := weler.renderEventXML(eventHandle)
				if err == nil {
					eventField["rawEventXML"] = eventXMLString
				} else {
					log.WithError(err).Error("Error rendering raw event XML")
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
				winevtcommon.EvtUpdateBookmark(bookmarkHandle, eventHandle)
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
func (weler *WinEventLogRenderer) renderMessageStrings(
	eventHandle winevtcommon.EventHandle,
	provider string,
) (string, error) {
	var message string
	publisherHandle, err := weler.publisherManager.GetPublisherHandle(provider)
	if err == nil {
		dwBufferUsed := uint32(0)
		if err = winevtcommon.EvtFormatMessage(
			publisherHandle,
			eventHandle,
			winevtcommon.EvtFormatMessageEvent,
			uint32(len(weler.bufferForRender)),
			&weler.bufferForRender[0],
			&dwBufferUsed,
		); err != nil {
			if err == winevtcommon.ErrorInsufficientBuffer && weler.bufferSize == -1 {
				log.Debugf(
					"Insufficient Buffer for rendering with length: %d. Retrying with Buffer of size: %d",
					len(weler.bufferForRender),
					dwBufferUsed,
				)
				weler.bufferForRender = make([]byte, dwBufferUsed)
				err = winevtcommon.EvtFormatMessage(
					publisherHandle,
					eventHandle,
					winevtcommon.EvtFormatMessageEvent,
					uint32(len(weler.bufferForRender)),
					&weler.bufferForRender[0],
					&dwBufferUsed,
				)
			} else {
				log.WithError(err).Error("Error formatting message strings")
			}
		}
		if err == nil {
			message, err = wincommon.ExtractString(weler.bufferForRender[:])
		}
	}
	return message, err
}

func (weler *WinEventLogRenderer) renderSystemData(eventHandle winevtcommon.EventHandle) (interface{}, error) {
	systemProperties := make(map[string]interface{})
	var err error
	if err == nil {
		var buf []byte
		var propertyCount uint32
		buf, _, propertyCount, err = weler.render(
			eventHandle,
			weler.evtCreateRenderContextHandle,
			winevtcommon.EvtRenderEventValues,
		)
		if err == nil {
			propertyId := 0
			start := 0
			end := SizeOfEvtVariant
			for propertyId < int(propertyCount) {
				bufSlice := buf[start:end]
				evtVariant := (*winevtcommon.EvtVariant)(unsafe.Pointer(&bufSlice[0]))
				data := evtVariant.GetData()
				if data != nil {
					systemProperties[winevtcommon.SystemPropertyIds[propertyId]] = data
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

func (weler *WinEventLogRenderer) renderEventXML(eventHandle winevtcommon.EventHandle) (string, error) {
	var err error
	var buf []byte
	var eventXmlString string
	buf, _, _, err = weler.render(eventHandle, 0, winevtcommon.EvtRenderEventXml)
	if err == nil {
		eventXmlString, err = wincommon.ExtractString(buf)
	}
	return eventXmlString, err
}

func (weler *WinEventLogRenderer) render(
	eventHandle winevtcommon.EventHandle,
	eventRenderContextHandle winevtcommon.EventRenderContextHandle,
	flags winevtcommon.EvtRenderFlag,
) ([]byte, uint32, uint32, error) {
	dwBufferUsed := uint32(0)
	dwPropertyCount := uint32(0)
	err := winevtcommon.EvtRender(
		eventRenderContextHandle,
		eventHandle,
		flags,
		uint32(len(weler.bufferForRender)),
		&weler.bufferForRender[0],
		&dwBufferUsed,
		&dwPropertyCount,
	)
	if err != nil && err == winevtcommon.ErrorInsufficientBuffer && weler.bufferSize == -1 {
		log.Debugf(
			"Insufficient Buffer for rendering with length: %d. Retrying with Buffer of size: %d",
			len(weler.bufferForRender),
			dwBufferUsed,
		)
		weler.bufferForRender = make([]byte, dwBufferUsed)
		err = winevtcommon.EvtRender(
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

func getRecordIdAndProviderName(systemData interface{}) (string, string, error) {
	providerName := cast.ToString(getPropertyFromSystemData(systemData, winevtcommon.EvtSystemProviderName))
	recordId := cast.ToUint64(getPropertyFromSystemData(systemData, winevtcommon.EvtSystemEventRecordId))
	computerName := cast.ToString(getPropertyFromSystemData(systemData, winevtcommon.EvtSystemComputer))
	channel := cast.ToString(getPropertyFromSystemData(systemData, winevtcommon.EvtSystemChannel))
	timeCreated := cast.ToTime(getPropertyFromSystemData(systemData, winevtcommon.EvtSystemTimeCreated))
	recordIdString := computerName + "::" + channel + "::" +
		strconv.FormatUint(recordId, 10) + "::" +
		strconv.FormatInt(wincommon.ConvertTimeToLong(timeCreated), 10)
	return recordIdString, providerName, nil
}

func getPropertyFromSystemData(
	systemData interface{},
	propertyId winevtcommon.EvtSystemPropertyId,
) interface{} {
	return cast.ToStringMap(systemData)[winevtcommon.SystemPropertyIds[propertyId]]
}

func NewWinEventLogRenderer(bufferSize int, rawEventPopulationStrategy RawEventPopulationStrategy) *WinEventLogRenderer {
	bufferSizeValue := BufferSizeDefault
	if bufferSize != -1 {
		bufferSizeValue = uint32(bufferSize)
	}
	publisherManager := &winEventLogPublisherManager{
		providerToPublisherMetadataHandle: map[string]winevtcommon.PublisherMetadataHandle{},
	}
	return &WinEventLogRenderer{
		bufferSize:                 bufferSize,
		rawEventPopulationStrategy: rawEventPopulationStrategy,
		bufferForRender:            make([]byte, bufferSizeValue),
		publisherManager:           publisherManager,
	}
}

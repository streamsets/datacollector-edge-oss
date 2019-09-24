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
package eventlogging

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/AllenDang/w32"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	wincommon "github.com/streamsets/datacollector-edge/stages/origins/windows/common"
	syswin "golang.org/x/sys/windows"
	"io"
	"strconv"
	"unsafe"
)

const (
	//https://msdn.microsoft.com/en-us/library/windows/desktop/aa363674(v=vs.85).aspx
	// -> Max buffer size allowed 0x7ffff (which is equal to 524287)
	BufferSizeMax = uint32(524287)
	EventSize     = uint32(unsafe.Sizeof(w32.EVENTLOGRECORD{}))
)

//Windows Event Log Reader - https://docs.microsoft.com/en-us/windows/desktop/wes/windows-event-log

// Event Logging - https://docs.microsoft.com/en-us/windows/desktop/EventLog/event-logging
type eventLoggingReader struct {
	*common.BaseStage
	*wincommon.BaseEventLogReader
	offset      uint32
	emptyLog    bool
	knownOffset bool
	handle      w32.HANDLE
	buffer      []byte
}

type eventLoggingRecord struct {
	w32.EVENTLOGRECORD
	SourceName   string
	ComputerName string
	MsgStrings   []string
	Message      string
	Category     string
	SIDInfo      *wincommon.SIDInfo
}

func NewEventLoggingReader(
	baseStage *common.BaseStage,
	logName string,
	mode wincommon.EventLogReaderMode,
	bufferSize int,
	maxBatchSize int,
	lastSourceOffset string,
) (wincommon.EventLogReader, error) {
	offset := uint32(0)
	knownOffset := false

	if uint32(bufferSize) > BufferSizeMax {
		err := errors.New(fmt.Sprintf("Invalid Buffer Size : %d should be < %d", bufferSize, BufferSizeMax))
		baseStage.GetStageContext().ReportError(err)
		log.WithError(err).WithField("bufferSize", bufferSize).Error("Wrong Buffer Size")
		return nil, err
	}

	if bufferSize == -1 {
		bufferSize = int(BufferSizeMax)
	}

	if lastSourceOffset != "" {
		off, err := strconv.ParseUint(lastSourceOffset, 10, 32)
		if err != nil {
			baseStage.GetStageContext().ReportError(err)
			log.WithError(err).WithField("offset", lastSourceOffset).Error("Error while parsing offset")
			return nil, err
		}
		offset = uint32(off)
		knownOffset = true
	}

	return &eventLoggingReader{
		BaseStage: baseStage,
		BaseEventLogReader: &wincommon.BaseEventLogReader{
			Log:          logName,
			Mode:         mode,
			MaxBatchSize: maxBatchSize,
		},
		offset:      offset,
		emptyLog:    false,
		knownOffset: knownOffset,
		buffer:      make([]byte, bufferSize),
	}, nil
}

func (elreader *eventLoggingReader) Open() error {
	log.Debugf("eventLoggingReader[%s] - Opening\n", elreader.Log)
	w32Handle := w32.OpenEventLog(`\\localhost`, elreader.Log)
	if w32Handle == 0 {
		return errors.New(fmt.Sprintf("could not open event log reader for '%s'", elreader.Log))
	} else {
		elreader.handle = w32Handle
		return elreader.determineFirstEventToRead()
	}
}

func (elreader *eventLoggingReader) Read() ([]api.Record, error) {
	records := make([]api.Record, 0)
	var flags uint32
	log.WithFields(log.Fields{
		"emptyLog":   elreader.Log,
		"offset":     elreader.offset,
		"maxRecords": elreader.MaxBatchSize,
	}).Debug("Attempting to read")
	if elreader.emptyLog {
		//special case where the event log is empty at the time of opening the reader
		flags = w32.EVENTLOG_FORWARDS_READ | w32.EVENTLOG_SEQUENTIAL_READ
	} else {
		flags = w32.EVENTLOG_FORWARDS_READ | w32.EVENTLOG_SEEK_READ
	}
	if events, err := elreader.read(flags, uint32(elreader.offset), elreader.MaxBatchSize); err == nil {
		if len(events) > 0 {
			elreader.offset = events[len(events)-1].RecordNumber + 1
			log.WithFields(log.Fields{
				"log":              elreader.Log,
				"eventRecordsRead": len(events),
				"lastRecordNumber": events[len(events)-1].RecordNumber,
			}).Debug()
			//after we read a record, we must rest the emtpyLog flag in case it is set
			elreader.emptyLog = false
			for _, event := range events {
				record, err := elreader.createRecord(event)
				if err != nil {
					log.WithError(err).Errorf("Error creating record for Record Number : %d", event.RecordNumber)
				}
				records = append(records, record)
			}
		} else {
			log.WithField("log", elreader.Log).Debug("No event records to read")
		}
		return records, nil
	} else {
		return nil, err
	}
}

// returns -1 if unknown
func (elreader *eventLoggingReader) GetCurrentOffset() string {
	return strconv.FormatUint(uint64(elreader.offset), 10)
}

func (elreader *eventLoggingReader) Close() error {
	log.Debug("eventLoggingReader[%s] - Closing\n", elreader.Log)
	if w32.CloseEventLog(elreader.handle) {
		return nil
	} else {
		return fmt.Errorf("could not close event log reader %+v", elreader)
	}
	ReleaseResourceLibraries()
	return nil
}

// Private Methods

func (elreader *eventLoggingReader) determineFirstEventToRead() error {
	elReaderLogger := log.WithFields(log.Fields{"log": elreader.Log})
	if !elreader.knownOffset {
		elReaderLogger.Debug("First event record number to read not known, locating...")
		var flags uint32
		if elreader.Mode == wincommon.ReadAll {
			flags = w32.EVENTLOG_FORWARDS_READ | w32.EVENTLOG_SEQUENTIAL_READ
		} else if elreader.Mode == wincommon.ReadNew {
			flags = w32.EVENTLOG_BACKWARDS_READ | w32.EVENTLOG_SEQUENTIAL_READ
		} else {
			return fmt.Errorf("invalid mode, %s", elreader.Mode)
		}
		if events, err := elreader.read(flags, 0, 1); err == nil {
			if len(events) == 0 {
				elreader.offset = 0
				elReaderLogger.Warn("Event log is empty, will start reading from first record to be written")
				//Handle special case that log is empty at the moment, we must do a forward/sequential read
				//to acquire the first avail record, then seek as usual
				elreader.emptyLog = true
			} else {
				elreader.offset = events[0].RecordNumber
				if elreader.Mode == wincommon.ReadNew {
					elreader.offset += 1
				}
				elReaderLogger.WithField("offset", elReaderLogger).Debug("First event record number to read")
			}
			return nil
		} else {
			return err
		}
	} else {
		elReaderLogger.WithField("offset", elreader.offset).Debug("Verifying first event record number to read")
		var flags uint32 = w32.EVENTLOG_FORWARDS_READ | w32.EVENTLOG_SEEK_READ
		if events, err := elreader.read(flags, elreader.offset, 1); err == nil {
			if len(events) != 0 {
				elReaderLogger.WithField("offset", elreader.offset).Debug("Verified first event record number to read")
				return nil
			} else {
				if events, err := elreader.read(flags, elreader.offset-1, 1); err == nil {
					if len(events) != 0 {
						elReaderLogger.WithField("offset", elreader.offset).Debug(
							"Verified first event record number to read (not yet available)",
						)
						return nil
					} else {
						elReaderLogger.Warn("Verification of first event record to read failed, repositioning")
						// if offset and offset - 1 do not return a record it means we have a gap and we should
						// start from the beginning of the log after reporting the issue
						flags = w32.EVENTLOG_FORWARDS_READ | w32.EVENTLOG_SEQUENTIAL_READ
						if events, err := elreader.read(flags, 0, 1); err == nil {
							if len(events) == 0 {
								elReaderLogger.Warn("Repositioning, event log is empty, will start reading " +
									"from first record to be written")
								//Handle special case that log is empty at the moment, we must do a forward/sequential
								//read to acquire the first avail record, then seek as usual
								elreader.emptyLog = true
							} else {
								elreader.offset = events[0].RecordNumber
								elReaderLogger.WithField("offset", elreader.offset).Warn(
									"Repositioning, first record found, will start with it",
								)
							}
							return nil
						} else {
							return err
						}
						return nil
					}
				} else {
					return err
				}
			}
			return nil
		} else {
			return err
		}
	}
}

func (elreader *eventLoggingReader) createRecord(event eventLoggingRecord) (api.Record, error) {
	recordId := event.ComputerName + "::" + elreader.Log + "::" +
		strconv.FormatUint(uint64(event.RecordNumber), 10) + "::" +
		strconv.FormatUint(uint64(event.TimeGenerated), 10)
	recordVal := map[string]interface{}{
		"ComputerName":  event.ComputerName,
		"RecordNumber":  event.RecordNumber,
		"DataOffset":    event.DataOffset,
		"DataLength":    event.DataLength,
		"Category":      event.EventCategory,
		"EventId":       event.EventID,
		"EventType":     event.EventType,
		"SourceName":    event.SourceName,
		"LogName":       elreader.Log,
		"StringOffset":  event.StringOffset,
		"Reserved":      event.Reserved,
		"TimeGenerated": event.TimeGenerated,
		"TimeWritten":   event.TimeWritten,
		"ReservedFlags": event.ReservedFlags,
		"UserSidLength": event.UserSidLength,
		"UserSidOffset": event.UserSidOffset,
		"NumStrings":    event.NumStrings,
		"Length":        event.Length,
		"MsgStrings":    event.MsgStrings,
		"Message":       event.Message,
	}

	if event.SIDInfo != nil {
		recordVal["SIDInfo"] = map[string]interface{}{
			"username": event.SIDInfo.Name,
			"domain":   event.SIDInfo.Domain,
			"sidType": map[string]interface{}{
				"Type":         uint32(event.SIDInfo.SIDType),
				"MappedString": event.SIDInfo.SIDType.GetSidTypeString(),
			},
		}
	}

	return elreader.GetStageContext().CreateRecord(recordId, recordVal)
}

// we don't return EOF, we just return an empty slice
func (elreader *eventLoggingReader) read(flags uint32, offset uint32, maxRecords int) ([]eventLoggingRecord, error) {
	events := make([]eventLoggingRecord, 0, maxRecords)
	buffer := elreader.buffer
	var read, needs uint32
	if w32.ReadEventLog(elreader.handle, flags, offset, buffer, uint32(len(buffer)), &read, &needs) {
		if read == 0 {
			return events, nil
		}
		buffer = buffer[:read]
		reader := bytes.NewReader(buffer)
		for {
			if maxRecords > -1 && len(events) >= maxRecords {
				break
			}
			event := eventLoggingRecord{}
			w32EventPtr := (*w32.EVENTLOGRECORD)(unsafe.Pointer(&event))
			err := binary.Read(reader, binary.LittleEndian, w32EventPtr)
			if err == io.EOF {
				break
			} else if err != nil {
				return nil, err
			}
			bytesLeft := event.Length - EventSize
			eventData := make([]byte, bytesLeft, bytesLeft)
			_, err = io.ReadFull(reader, eventData)

			if err != nil {
				return nil, err
			}

			// extract source name and computer name
			strs := wincommon.ExtractStrings(eventData, uint16(2))
			event.SourceName = strs[0]
			event.ComputerName = strs[1]

			//This means we have SID information in the Event Log
			if event.UserSidLength > 0 {
				log.Debugf(
					"Trying to extract Sid Information for"+
						" Record number : %d,"+
						" Sid Offset: %d,"+
						" Sid Length : %d",
					event.RecordNumber, event.UserSidOffset, event.UserSidLength)
				sidOffset := event.UserSidOffset - EventSize
				sidPtr := (*syswin.SID)(unsafe.Pointer(&eventData[sidOffset]))
				sidString := sidPtr.String()
				if err != nil {
					log.WithError(err).Errorf(
						"Error extracting sid from Sid Offset:%d and Length:%d for record Number %d",
						event.UserSidOffset,
						event.UserSidLength,
						event.RecordNumber)
				} else {
					sid, err := syswin.StringToSid(sidString)
					if err != nil {
						log.WithError(err).Errorf("Error extracting SID from SID String %s, record Number %d",
							sidString,
							event.RecordNumber)
					} else {
						sidInfo, err := wincommon.GetSidInfo(sid)
						if err == nil {
							event.SIDInfo = sidInfo
						} else {
							log.WithError(err).Errorf(
								"Error Lookup Account Name for SID String: %s record Number %d",
								sidString,
								event.RecordNumber,
							)
						}
					}
				}

			} else {
				log.Infof("No SID Information in the windows event log record number %d", event.RecordNumber)
			}

			// extract message strings
			if event.NumStrings > 0 {
				strOffset := event.StringOffset - EventSize
				eventData := eventData[strOffset:]
				event.MsgStrings = wincommon.ExtractStrings(eventData, uint16(event.NumStrings))
			}
			event.Message = messageF(findEventMessageTemplate(elreader.Log, &event), event.MsgStrings)
			event.Category = findEventCategory(elreader.Log, &event)
			events = append(events, event)
		}
	}
	return events, nil
}

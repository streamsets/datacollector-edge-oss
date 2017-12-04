// +build 386 windows,amd64 windows

/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Copied from https://github.com/streamsets/windataextractor/tree/master/dev/src/lib/win/eventlog
package windows

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/AllenDang/w32"
	log "github.com/sirupsen/logrus"
	"io"
	"syscall"
	"unsafe"
)

const (
	bufferSize uint32 = 10240
	eventSize         = uint32(unsafe.Sizeof(w32.EVENTLOGRECORD{}))
)

type EventLogReaderMode string

const (
	READ_ALL = EventLogReaderMode("ALL")
	READ_NEW = EventLogReaderMode("NEW")
)

type EventLogReader struct {
	log         string
	mode        EventLogReaderMode
	offset      uint32
	emptyLog    bool
	knownOffset bool
	handle      w32.HANDLE
}

type EventLogRecord struct {
	w32.EVENTLOGRECORD
	SourceName   string
	ComputerName string
	MsgStrings   []string
	Message      string
	Category     string
}

func NewReader(logName string, mode EventLogReaderMode, initialOffset uint32, knownOffset bool) *EventLogReader {
	return &EventLogReader{logName, mode, initialOffset, false, knownOffset, 0}
}

func (elreader *EventLogReader) Open() error {
	log.Debug("EventLogReader[%s] - Opening\n", elreader.log)
	w32Handle := w32.OpenEventLog(`\\localhost`, elreader.log)
	if w32Handle == 0 {
		return fmt.Errorf("Could not open event log reader for '%s'", elreader.log)
	} else {
		elreader.handle = w32Handle
		return elreader.determineFirstEventToRead()
	}
}

// returns -1 if unknown
func (elreader *EventLogReader) GetCurrentOffset() uint32 {
	return elreader.offset
}

func (elreader *EventLogReader) Close() error {
	log.Debug("EventLogReader[%s] - Closing\n", elreader.log)
	if w32.CloseEventLog(elreader.handle) {
		return nil
	} else {
		return fmt.Errorf("Could not close event log reader %+v", elreader)
	}
}

func (elreader *EventLogReader) determineFirstEventToRead() error {
	elReaderLogger := log.WithFields(log.Fields{"log": elreader.log})
	if !elreader.knownOffset {
		elReaderLogger.Debug("First event record number to read not known, locating...")
		var flags uint32
		if elreader.mode == READ_ALL {
			flags = w32.EVENTLOG_FORWARDS_READ | w32.EVENTLOG_SEQUENTIAL_READ
		} else if elreader.mode == READ_NEW {
			flags = w32.EVENTLOG_BACKWARDS_READ | w32.EVENTLOG_SEQUENTIAL_READ
		} else {
			return fmt.Errorf("Invalid mode, %s", elreader.mode)
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
				if elreader.mode == READ_NEW {
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

// we don't return EOF, we just return an empty slice
func (elreader *EventLogReader) read(flags uint32, offset uint32, maxRecords int) ([]EventLogRecord, error) {
	events := make([]EventLogRecord, 0, maxRecords)
	buffer := make([]byte, bufferSize)
	var read, needs uint32
	if w32.ReadEventLog(elreader.handle, flags, offset, buffer, bufferSize, &read, &needs) {
		if read == 0 {
			return events, nil
		}
		buffer = buffer[:read]
		reader := bytes.NewReader(buffer)
		for {
			if maxRecords > -1 && len(events) >= maxRecords {
				break
			}
			event := EventLogRecord{}
			w32EventPtr := (*w32.EVENTLOGRECORD)(unsafe.Pointer(&event))
			err := binary.Read(reader, binary.LittleEndian, w32EventPtr)
			if err == io.EOF {
				break
			} else if err != nil {
				return nil, err
			}
			bytesLeft := event.Length - eventSize
			eventData := make([]byte, bytesLeft, bytesLeft)
			_, err = io.ReadFull(reader, eventData)
			if err != nil {
				return nil, err
			}
			// extract source name and computer name
			strs := extractStrings(eventData, uint16(2))
			event.SourceName = strs[0]
			event.ComputerName = strs[1]
			// extract message strings
			if event.NumStrings > 0 {
				strOffset := event.StringOffset - eventSize
				eventData := eventData[strOffset:]
				event.MsgStrings = extractStrings(eventData, uint16(event.NumStrings))
			}
			event.Message = messageF(findEventMessageTemplate(elreader.log, &event), event.MsgStrings)
			event.Category = findEventCategory(elreader.log, &event)
			events = append(events, event)
		}
	}
	return events, nil
}

func (elreader *EventLogReader) Read(maxRecords int) ([]EventLogRecord, error) {
	var flags uint32
	log.WithFields(log.Fields{
		"emptyLog":   elreader.log,
		"offset":     elreader.offset,
		"maxRecords": maxRecords,
	}).Debug("Attempting to read")
	if elreader.emptyLog {
		//special case where the event log is empty at the time of opening the reader
		flags = w32.EVENTLOG_FORWARDS_READ | w32.EVENTLOG_SEQUENTIAL_READ
	} else {
		flags = w32.EVENTLOG_FORWARDS_READ | w32.EVENTLOG_SEEK_READ
	}
	if events, err := elreader.read(flags, uint32(elreader.offset), maxRecords); err == nil {
		if len(events) > 0 {
			elreader.offset = events[len(events)-1].RecordNumber + 1
			log.WithFields(log.Fields{
				"log":              elreader.log,
				"eventRecordsRead": len(events),
				"lastRecordNumber": events[len(events)-1].RecordNumber,
			}).Debug()
			//after we read a record, we must rest the emtpyLog flag in case it is set
			elreader.emptyLog = false
		} else {
			log.WithField("log", elreader.log).Debug("No event records to read")
		}
		return events, nil
	} else {
		return nil, err
	}
}

func extractStrings(byteData []byte, stringCount uint16) (strs []string) {
	strs = make([]string, 0, stringCount)
	wordArray := make([]uint16, len(byteData)/2)
	err := binary.Read(bytes.NewReader(byteData), binary.LittleEndian, wordArray)
	if err != nil {
		log.WithError(err).Fatal()
	}
	pos := 0
	for idx, value := range wordArray {
		if value == 0 {
			str := syscall.UTF16ToString(wordArray[pos:idx])
			strs = append(strs, str)
			pos = idx + 1
			stringCount--
			if stringCount == 0 {
				break
			}
		}
	}
	return strs
}

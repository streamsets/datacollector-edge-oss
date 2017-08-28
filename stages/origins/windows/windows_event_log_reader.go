// +build 386 windows,amd64 windows

package windows

import (
	"errors"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"log"
	"runtime"
	"strconv"
)

const (
	LIBRARY          = "streamsets-datacollector-windows-lib"
	STAGE_NAME       = "com_streamsets_pipeline_stage_origin_windows_WindowsEventLogDSource"
	WINDOWS          = "windows"
	LOG_NAME_CONFIG  = "logName"
	READ_MODE_CONFIG = "readMode"
)

type WindowsEventLogSource struct {
	*common.BaseStage
	logName        string
	readMode       EventLogReaderMode
	eventLogReader *EventLogReader
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &WindowsEventLogSource{BaseStage: &common.BaseStage{}}
	})
}

func (wel *WindowsEventLogSource) Init(stageContext api.StageContext) error {
	if err := wel.BaseStage.Init(stageContext); err != nil {
		return err
	}
	stageConfig := wel.GetStageConfig()

	if runtime.GOOS != WINDOWS {
		return errors.New("Windows Event Log Source should be run on Windows OS")
	}

	for _, config := range stageConfig.Configuration {
		value := wel.GetStageContext().GetResolvedValue(config.Value)
		switch config.Name {
		case LOG_NAME_CONFIG:
			logName := value.(string)
			if !(logName == SYSTEM || logName == APPLICATION || logName == SECURITY) {
				return errors.New("Unsupported Log Name :" + logName)
			}
			wel.logName = logName
		case READ_MODE_CONFIG:
			wel.readMode = EventLogReaderMode(value.(string))
		}
	}
	return nil
}

func (wel *WindowsEventLogSource) Produce(
	lastSourceOffset string,
	maxBatchSize int,
	batchMaker api.BatchMaker,
) (string, error) {
	if wel.eventLogReader == nil {
		if lastSourceOffset == "" {
			wel.eventLogReader = NewReader(wel.logName, wel.readMode, 0, false)
		} else {
			off, err := strconv.ParseUint(lastSourceOffset, 10, 32)
			if err != nil {
				wel.GetStageContext().ReportError(err)
				log.Printf("[ERROR] Error happened on Parsing Offset '%s' : %s\n", lastSourceOffset, err.Error())
				return lastSourceOffset, err
			}
			wel.eventLogReader = NewReader(wel.logName, wel.readMode, uint32(off), true)
		}
		if err := wel.eventLogReader.Open(); err != nil {
			wel.GetStageContext().ReportError(err)
			log.Printf("[ERROR] Error happened on Opening Event Reader : %s\n", err.Error())
			return lastSourceOffset, err
		}
	}

	if events, err := wel.eventLogReader.Read(maxBatchSize); err == nil {
		if len(events) > 0 {
			for _, event := range events {
				er := wel.createRecordAndAddToBatch(event, batchMaker)
				if er != nil {
					log.Printf("[ERROR] Error when creating record : %s\n", er.Error())
					wel.GetStageContext().ReportError(er)
					return lastSourceOffset, er
				}
			}
		}
	} else {
		wel.GetStageContext().ReportError(err)
		log.Printf("[ERROR] Error happened on Event Log Read : %s\n", err.Error())
		return lastSourceOffset, err
	}

	return strconv.FormatUint(uint64(wel.eventLogReader.GetCurrentOffset()), 10), nil
}

func (wel *WindowsEventLogSource) createRecordAndAddToBatch(event EventLogRecord, batchMaker api.BatchMaker) error {
	recordId := event.ComputerName + "::" + wel.logName + "::" + string(event.EventID)
	recordVal := map[string]interface{}{
		"ComputerName":  event.ComputerName,
		"RecordNumber":  event.RecordNumber,
		"DataOffset":    event.DataOffset,
		"DataLength":    event.DataLength,
		"Category":      event.EventCategory,
		"EventId":       event.EventID,
		"SourceName":    event.SourceName,
		"LogName":       wel.logName,
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
	record, er := wel.GetStageContext().CreateRecord(recordId, recordVal)
	if er != nil {
		return er
	}
	batchMaker.AddRecord(record)
	return nil
}

func (wel *WindowsEventLogSource) Destroy() error {
	err := wel.eventLogReader.Close()
	if err != nil {
		log.Printf("[ERROR] Error %s when closing event reader", err.Error())
	}
	ReleaseResourceLibraries()
	return nil
}

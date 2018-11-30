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
package filetail

import (
	"bufio"
	"fmt"
	"github.com/hpcloud/tail"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/recordio/delimitedrecord"
	"github.com/streamsets/datacollector-edge/container/util"
	"github.com/streamsets/datacollector-edge/stages/lib/dataparser"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	Library             = "streamsets-datacollector-basic-lib"
	StageName           = "com_streamsets_pipeline_stage_origin_logtail_FileTailDSource"
	ConfGroupFiles      = "FILES"
	ConfFileInfos       = "conf.fileInfos"
	ConfMaxWaitTimeSecs = "conf.maxWaitTimeSecs"
	ConfBatchSize       = "conf.batchSize"
	ConfDataFormat      = "conf.dataFormat"
	ErrorTail20         = "File path cannot be null or empty"
	ErrorTail02         = "File path doesn't exist: %s"
)

type FileTailOrigin struct {
	*common.BaseStage
	Conf       FileTailConfigBean `ConfigDefBean:"name=conf"`
	csvHeaders []*api.Field
}

var lastLineReadAfterStop string

type FileTailConfigBean struct {
	BatchSize        float64                           `ConfigDef:"type=NUMBER,required=true"`
	MaxWaitTimeSecs  float64                           `ConfigDef:"type=NUMBER,required=true"`
	FileInfos        []FileInfo                        `ConfigDef:"type=MODEL" ListBeanModel:"name=fileInfos"`
	DataFormat       string                            `ConfigDef:"type=STRING,required=true"`
	DataFormatConfig dataparser.DataParserFormatConfig `ConfigDefBean:"dataFormatConfig"`
}

type FileInfo struct {
	FileFullPath string `ConfigDef:"type=STRING,required=true"`
}

func init() {
	stagelibrary.SetCreator(Library, StageName, func() api.Stage {
		return &FileTailOrigin{BaseStage: &common.BaseStage{}}
	})
}

func (f *FileTailOrigin) Init(stageContext api.StageContext) []validation.Issue {
	issues := f.BaseStage.Init(stageContext)
	// validate file path
	if len(f.Conf.FileInfos) == 0 || f.Conf.FileInfos[0].FileFullPath == "" {
		issues = append(issues, stageContext.CreateConfigIssue(ErrorTail20, ConfGroupFiles, ConfFileInfos))
		return issues
	}

	if _, err := os.Stat(f.Conf.FileInfos[0].FileFullPath); os.IsNotExist(err) {
		issues = append(issues, stageContext.CreateConfigIssue(
			fmt.Sprintf(ErrorTail02, f.Conf.FileInfos[0].FileFullPath),
			ConfGroupFiles,
			ConfFileInfos,
		))
		return issues
	}
	log.WithField("file", f.Conf.FileInfos[0].FileFullPath).Debug("Reading file")

	if f.Conf.DataFormat == "DELIMITED" && f.Conf.DataFormatConfig.CsvHeader == delimitedrecord.WithHeader {
		file, _ := os.Open(f.Conf.FileInfos[0].FileFullPath)
		defer util.CloseFile(file)
		bufReader := bufio.NewReader(file)
		headerLine, err := bufReader.ReadString('\n')
		if err == nil {
			columns := strings.Split(headerLine, ",")
			f.csvHeaders = make([]*api.Field, len(columns))
			for i, col := range columns {
				headerField, _ := api.CreateStringField(col)
				f.csvHeaders[i] = headerField
			}
		}
	}

	return f.Conf.DataFormatConfig.Init(f.Conf.DataFormat, stageContext, issues)
}

func (f *FileTailOrigin) Produce(
	lastSourceOffset *string,
	maxBatchSize int,
	batchMaker api.BatchMaker,
) (*string, error) {
	log.WithField("lastSourceOffset", lastSourceOffset).Debug("Produce called")

	batchSize := math.Min(float64(maxBatchSize), f.Conf.BatchSize)

	tailConfig := tail.Config{
		MustExist: true,
		Follow:    true,
		Logger:    tail.DiscardingLogger,
	}

	if util.IsStringEmpty(lastSourceOffset) {
		intOffset, _ := strconv.ParseInt(*lastSourceOffset, 10, 64)
		tailConfig.Location = &tail.SeekInfo{Offset: intOffset, Whence: io.SeekStart}
	}

	tailObj, err := tail.TailFile(f.Conf.FileInfos[0].FileFullPath, tailConfig)
	if err != nil {
		return lastSourceOffset, err
	}

	var currentOffset int64
	recordCount := float64(0)
	skippedLines := 0
	timeout := time.NewTimer(time.Duration(f.Conf.MaxWaitTimeSecs) * time.Second)
	defer timeout.Stop()
	end := false
	for !end {
		select {
		case line := <-tailObj.Lines:
			if line != nil {
				if recordCount == 0 && lastLineReadAfterStop == line.Text {
					// Duplicate line from last batch, due to offset issue with tail library
					log.WithField("data", line.Text).Warn("Ignoring duplicate line from last batch")
				} else {
					if f.Conf.DataFormat == "DELIMITED" && lastSourceOffset == nil && recordCount == 0 {
						if skippedLines < int(f.Conf.DataFormatConfig.CsvSkipStartLines) {
							skippedLines++
							break
						} else if skippedLines == 0 && (f.Conf.DataFormatConfig.CsvHeader == delimitedrecord.WithHeader ||
							f.Conf.DataFormatConfig.CsvHeader == delimitedrecord.IgnoreHeader) {
							skippedLines++
							break
						}
					}
					err = f.parseLine(line.Text, batchMaker, &recordCount)
					if err != nil {
						f.GetStageContext().ReportError(err)
					}

					if recordCount >= batchSize {
						currentOffset, err = tailObj.Tell()
						if err != nil {
							log.WithError(err).Error("Failed to get file offset information")
							f.GetStageContext().ReportError(err)
						}
						end = true
					}
				}
			}
		case <-timeout.C:
			currentOffset, err = tailObj.Tell()
			if err != nil {
				log.WithError(err).Error("Failed to get file offset information")
				f.GetStageContext().ReportError(err)
			}
			end = true
		}
	}

	f.stopTailing(tailObj, batchMaker, &recordCount)

	stringOffset := strconv.FormatInt(currentOffset, 10)

	return &stringOffset, err
}

func (f *FileTailOrigin) parseLine(
	lineText string,
	batchMaker api.BatchMaker,
	recordCount *float64,
) error {
	sourceId := common.CreateRecordId("fileTail", int(*recordCount))
	record, err := f.Conf.DataFormatConfig.RecordCreator.CreateRecord(
		f.GetStageContext(),
		strings.Replace(lineText, "\n", "", 1),
		sourceId,
		f.csvHeaders,
	)
	if err != nil {
		f.GetStageContext().ReportError(err)
		return nil
	}
	batchMaker.AddRecord(record)
	*recordCount++
	return nil
}

func (f *FileTailOrigin) stopTailing(
	tailObj *tail.Tail,
	batchMaker api.BatchMaker,
	recordCount *float64,
) error {
	lastLineReadAfterStop = ""
	tailObj.Kill(nil)
	time.Sleep(time.Microsecond)
	end := false
	for !end {
		select {
		case line, ok := <-tailObj.Lines:
			if !ok {
				end = true
			} else if line != nil {
				err := f.parseLine(line.Text, batchMaker, recordCount)
				if err != nil {
					return err
				}
				lastLineReadAfterStop = line.Text
			}
		default:
			end = true
		}
	}
	return nil
}

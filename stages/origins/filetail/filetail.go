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
package filetail

import (
	"bytes"
	"github.com/hpcloud/tail"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/recordio"
	"github.com/streamsets/datacollector-edge/container/util"
	"github.com/streamsets/datacollector-edge/stages/lib/dataparser"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"io"
	"strconv"
	"time"
)

const (
	LIBRARY             = "streamsets-datacollector-basic-lib"
	STAGE_NAME          = "com_streamsets_pipeline_stage_origin_logtail_FileTailDSource"
	ConfGroupFiles      = "FILES"
	ConfFileInfos       = "conf.fileInfos"
	ConfMaxWaitTimeSecs = "conf.maxWaitTimeSecs"
	ConfBatchSize       = "conf.batchSize"
	ConfDataFormat      = "conf.dataFormat"
	ErrorTail20         = "File path cannot be null or empty"
)

type FileTailOrigin struct {
	*common.BaseStage
	Conf FileTailConfigBean `ConfigDefBean:"name=conf"`
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
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
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
	log.WithField("file", f.Conf.FileInfos[0].FileFullPath).Debug("Reading file")
	return f.Conf.DataFormatConfig.Init(f.Conf.DataFormat, stageContext, issues)
}

func (f *FileTailOrigin) Produce(
	lastSourceOffset *string,
	maxBatchSize int,
	batchMaker api.BatchMaker,
) (*string, error) {
	log.WithField("lastSourceOffset", lastSourceOffset).Debug("Produce called")

	recordReaderFactory := f.Conf.DataFormatConfig.RecordReaderFactory

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
	end := false
	for !end {
		select {
		case line := <-tailObj.Lines:
			if line != nil {
				if recordCount == 0 && lastLineReadAfterStop == line.Text {
					// Duplicate line from last batch, due to offset issue with tail library
					log.WithField("data", line.Text).Warn("Ignoring duplicate line from last batch")
				} else {
					err = f.parseLine(recordReaderFactory, line.Text, batchMaker, &recordCount)
					if err != nil {
						f.GetStageContext().ReportError(err)
					}

					if recordCount >= f.Conf.BatchSize {
						currentOffset, err = tailObj.Tell()
						if err != nil {
							log.WithError(err).Error("Failed to get file offset information")
							f.GetStageContext().ReportError(err)
						}
						end = true
					}
				}
			}
		case <-time.After(time.Duration(f.Conf.MaxWaitTimeSecs) * time.Second):
			currentOffset, err = tailObj.Tell()
			if err != nil {
				log.WithError(err).Error("Failed to get file offset information")
				f.GetStageContext().ReportError(err)
			}
			end = true
		}
	}

	f.stopTailing(tailObj, recordReaderFactory, batchMaker, &recordCount)

	stringOffset := strconv.FormatInt(currentOffset, 10)

	return &stringOffset, err
}

func (f *FileTailOrigin) parseLine(
	recordReaderFactory recordio.RecordReaderFactory,
	lineText string,
	batchMaker api.BatchMaker,
	recordCount *float64,
) error {
	recordBuffer := bytes.NewBufferString(lineText)
	recordReader, err := recordReaderFactory.CreateReader(f.GetStageContext(), recordBuffer)
	if err != nil {
		log.WithError(err).Error("Failed to create record reader")
		return err
	}
	defer recordReader.Close()

	for {
		record, err := recordReader.ReadRecord()
		if err != nil {
			log.WithError(err).Error("Failed to parse raw data")
			return err
		}

		if record == nil {
			break
		}
		batchMaker.AddRecord(record)
		*recordCount++
	}
	return nil
}

func (f *FileTailOrigin) stopTailing(
	tailObj *tail.Tail,
	recordReaderFactory recordio.RecordReaderFactory,
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
				err := f.parseLine(recordReaderFactory, line.Text, batchMaker, recordCount)
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

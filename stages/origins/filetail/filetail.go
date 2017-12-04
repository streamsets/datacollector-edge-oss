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
	"github.com/hpcloud/tail"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"io"
	"strconv"
	"time"
)

const (
	LIBRARY                 = "streamsets-datacollector-basic-lib"
	STAGE_NAME              = "com_streamsets_pipeline_stage_origin_logtail_FileTailDSource"
	CONF_FILE_INFOS         = "conf.fileInfos"
	CONF_MAX_WAIT_TIME_SECS = "conf.maxWaitTimeSecs"
	CONF_BATCH_SIZE         = "conf.batchSize"
)

type FileTailOrigin struct {
	*common.BaseStage
	Conf FileTailConfigBean `ConfigDefBean:"name=conf"`
}

type FileTailConfigBean struct {
	BatchSize       float64    `ConfigDef:"type=NUMBER,required=true"`
	MaxWaitTimeSecs float64    `ConfigDef:"type=NUMBER,required=true"`
	FileInfos       []FileInfo `ConfigDef:"type=MODEL" ListBeanModel:"name=fileInfos"`
}

type FileInfo struct {
	FileFullPath string `ConfigDef:"type=STRING,required=true"`
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &FileTailOrigin{BaseStage: &common.BaseStage{}}
	})
}

func (f *FileTailOrigin) Init(stageContext api.StageContext) error {
	if err := f.BaseStage.Init(stageContext); err != nil {
		return err
	}
	log.WithField("file", f.Conf.FileInfos[0].FileFullPath).Debug("Reading file")
	return nil
}

func (f *FileTailOrigin) Produce(lastSourceOffset string, maxBatchSize int, batchMaker api.BatchMaker) (string, error) {
	log.WithField("lastSourceOffset", lastSourceOffset).Debug("Produce called")

	tailConfig := tail.Config{
		MustExist: true,
		Follow:    true,
		Logger:    tail.DiscardingLogger,
	}

	if lastSourceOffset != "" {
		intOffset, _ := strconv.ParseInt(lastSourceOffset, 10, 64)
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
				recordId := tailObj.Filename + "::" + strconv.FormatInt(currentOffset, 10)
				recordValue := map[string]interface{}{"text": line.Text}
				record, _ := f.GetStageContext().CreateRecord(recordId, recordValue)
				batchMaker.AddRecord(record)
				recordCount++
				if recordCount >= f.Conf.BatchSize {
					currentOffset, _ = tailObj.Tell()
					log.WithField("BatchSize", f.Conf.BatchSize).Debug("Calling stop due to MaxBatchSize")
					end = true
				}
			}
		case <-time.After(time.Duration(f.Conf.MaxWaitTimeSecs) * time.Second):
			log.WithField("MaxWaitTimeSecs", f.Conf.MaxWaitTimeSecs).Debug("Calling stop due to MaxWaitTimeSecs")
			currentOffset, _ = tailObj.Tell()
			end = true
		}
	}

	go f.stopTailing(tailObj)

	return strconv.FormatInt(currentOffset, 10), err
}

func (f *FileTailOrigin) stopTailing(tailObj *tail.Tail) {
	tailObj.Kill(nil)
	time.Sleep(time.Microsecond)

	end := false
	for !end {
		select {
		case _, ok := <-tailObj.Lines:
			if !ok {
				end = true
			}
		default:
			end = true
		}
	}

	tailObj.Wait()
}

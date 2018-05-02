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
package dev_random

import (
	"bytes"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/lib/dataparser"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
)

const (
	LIBRARY    = "streamsets-datacollector-dev-lib"
	STAGE_NAME = "com_streamsets_pipeline_stage_devtest_rawdata_RawDataDSource"
)

var randomOffset = "random"

type DevRawDataDSource struct {
	*common.BaseStage
	RawData             string                            `ConfigDef:"type=STRING,required=true"`
	StopAfterFirstBatch bool                              `ConfigDef:"type=BOOLEAN,required=true"`
	DataFormat          string                            `ConfigDef:"type=STRING,required=true"`
	DataFormatConfig    dataparser.DataParserFormatConfig `ConfigDefBean:"dataFormatConfig"`
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &DevRawDataDSource{BaseStage: &common.BaseStage{}}
	})
}

func (d *DevRawDataDSource) Init(stageContext api.StageContext) []validation.Issue {
	issues := d.BaseStage.Init(stageContext)
	log.Debug("DevRawDataDSource Init method")
	d.DataFormatConfig.Init(d.DataFormat, stageContext, issues)
	return issues
}

func (d *DevRawDataDSource) Produce(
	lastSourceOffset *string,
	maxBatchSize int,
	batchMaker api.BatchMaker,
) (*string, error) {
	var err error
	recordReaderFactory := d.DataFormatConfig.RecordReaderFactory
	recordBuffer := bytes.NewBufferString(d.RawData)
	recordReader, err := recordReaderFactory.CreateReader(d.GetStageContext(), recordBuffer)
	if err != nil {
		log.WithError(err).Error("Failed to create record reader")
		return nil, err
	}
	defer recordReader.Close()
	for {
		record, err := recordReader.ReadRecord()
		if err != nil {
			log.WithError(err).Error("Failed to parse raw data")
			return nil, err
		}

		if record == nil {
			break
		}
		batchMaker.AddRecord(record)
	}

	if d.StopAfterFirstBatch {
		return nil, err
	}

	return &randomOffset, err
}

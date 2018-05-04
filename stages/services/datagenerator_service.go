/*
 * Copyright 2018 StreamSets Inc.
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
package services

import (
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/dataformats"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/stages/lib/datagenerator"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"io"
)

type DataGeneratorServiceImpl struct {
	stageContext              api.StageContext
	DataFormat                string                                  `ConfigDef:"type=STRING,required=true"`
	DataGeneratorFormatConfig datagenerator.DataGeneratorFormatConfig `ConfigDefBean:"dataGeneratorFormatConfig"`
}

func init() {
	stagelibrary.SetServiceCreator(dataformats.DataFormatGeneratorServiceName, func() api.Service {
		return &DataGeneratorServiceImpl{}
	})
}

func (d *DataGeneratorServiceImpl) Init(stageContext api.StageContext) []validation.Issue {
	d.stageContext = stageContext
	issues := make([]validation.Issue, 0)
	log.Debug("DataGeneratorServiceImpl Init method")
	d.DataGeneratorFormatConfig.Init(d.DataFormat, stageContext, issues)
	return issues
}

func (d *DataGeneratorServiceImpl) GetGenerator(writer io.Writer) (dataformats.RecordWriter, error) {
	recordWriterFactory := d.DataGeneratorFormatConfig.RecordWriterFactory
	return recordWriterFactory.CreateWriter(d.stageContext, writer)
}

func (b *DataGeneratorServiceImpl) Destroy() error {
	return nil
}

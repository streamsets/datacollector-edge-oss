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
package services

import (
	"context"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cast"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/dataformats"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/el"
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

func (d *DataGeneratorServiceImpl) Destroy() error {
	return nil
}

func (d *DataGeneratorServiceImpl) IsWholeFileFormat() bool {
	return d.DataFormat == "WHOLE_FILE"
}

func (d *DataGeneratorServiceImpl) GetWholeFileName(record api.Record) (string, error) {
	recordContext := context.WithValue(context.Background(), el.RecordContextVar, record)
	result, err := d.stageContext.Evaluate(d.DataGeneratorFormatConfig.FileNameEL, "fileNameEl", recordContext)
	if err != nil {
		return "", err
	}
	return cast.ToString(result), nil
}

func (d *DataGeneratorServiceImpl) GetWholeFileExistsAction() string {
	return d.DataGeneratorFormatConfig.WholeFileExistsAction
}

func (d *DataGeneratorServiceImpl) GetIncludeChecksumInTheEvents() bool {
	return d.DataGeneratorFormatConfig.IncludeChecksumInTheEvents
}

func (d *DataGeneratorServiceImpl) GetChecksumAlgorithm() string {
	return d.DataGeneratorFormatConfig.ChecksumAlgorithm
}

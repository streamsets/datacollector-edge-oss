// Copyright 2019 StreamSets Inc.
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
package eventhubs

import (
	"bytes"
	"context"
	"github.com/Azure/azure-event-hubs-go"
	"github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/dataformats"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/lib/datagenerator"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
)

const (
	Library   = "streamsets-datacollector-azure-lib"
	StageName = "com_streamsets_pipeline_stage_destination_eventhubs_EventHubProducerDTarget"
)

type Destination struct {
	*common.BaseStage
	CommonConf     EventHubConfigBean         `ConfigDefBean:"commonConf"`
	ProducerConf   EventHubProducerConfigBean `ConfigDefBean:"producerConf"`
	eventHubClient *eventhub.Hub
}

type EventHubConfigBean struct {
	NamespaceName string `ConfigDef:"type=STRING,required=true"`
	EventHubName  string `ConfigDef:"type=STRING,required=true"`
	SasKeyName    string `ConfigDef:"type=STRING,required=true"`
	SasKey        string `ConfigDef:"type=STRING,required=true"`
}

type EventHubProducerConfigBean struct {
	DataFormat                string                                  `ConfigDef:"type=STRING,required=true"`
	DataGeneratorFormatConfig datagenerator.DataGeneratorFormatConfig `ConfigDefBean:"dataGeneratorFormatConfig"`
}

func init() {
	stagelibrary.SetCreator(Library, StageName, func() api.Stage {
		return &Destination{BaseStage: &common.BaseStage{}}
	})
}

func (dest *Destination) Init(stageContext api.StageContext) []validation.Issue {
	issues := dest.BaseStage.Init(stageContext)

	var err error
	dest.eventHubClient, err = eventhub.NewHubFromConnectionString(dest.CommonConf.SasKey)
	if err != nil {
		issues = append(issues, stageContext.CreateConfigIssue(err.Error()))
		return issues
	}

	issues = dest.ProducerConf.DataGeneratorFormatConfig.Init(dest.ProducerConf.DataFormat, stageContext, issues)
	return issues
}

func (dest *Destination) Write(batch api.Batch) error {
	recordWriterFactory := dest.ProducerConf.DataGeneratorFormatConfig.RecordWriterFactory

	records := batch.GetRecords()
	events := make([]*eventhub.Event, 0)

	for _, record := range records {
		recordBuffer := bytes.NewBuffer([]byte{})
		recordWriter, err := recordWriterFactory.CreateWriter(dest.GetStageContext(), recordBuffer)
		if err != nil {
			logrus.WithError(err).Error("Error creating writer")
			dest.GetStageContext().ToError(err, record)
			break
		}
		err = recordWriter.WriteRecord(record)
		if err != nil {
			logrus.WithError(err).Error("Error writing record")
			dest.GetStageContext().ToError(err, record)
			break
		}
		flushAndCloseWriter(recordWriter)

		events = append(events, eventhub.NewEvent(recordBuffer.Bytes()))
	}

	err := dest.eventHubClient.SendBatch(context.Background(), eventhub.NewEventBatch(events))
	if err != nil {
		logrus.WithError(err).Error("error while writing records to Azure Event Hub")
		for _, record := range records {
			dest.GetStageContext().ToError(err, record)
		}
		return nil
	}

	return nil
}

func flushAndCloseWriter(recordWriter dataformats.RecordWriter) {
	err := recordWriter.Flush()
	if err != nil {
		logrus.WithError(err).Error("Error flushing record writer")
	}

	err = recordWriter.Close()
	if err != nil {
		logrus.WithError(err).Error("Error closing record writer")
	}
}

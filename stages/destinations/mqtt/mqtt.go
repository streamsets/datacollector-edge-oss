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
package mqtt

import (
	"bytes"
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cast"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/dataformats"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/el"
	"github.com/streamsets/datacollector-edge/container/recordio"
	"github.com/streamsets/datacollector-edge/stages/lib/datagenerator"
	mqttlib "github.com/streamsets/datacollector-edge/stages/lib/mqtt"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
)

const (
	Library              = "streamsets-datacollector-basic-lib"
	StageName            = "com_streamsets_pipeline_stage_destination_mqtt_MqttClientDTarget"
	ErrorStageName       = "com_streamsets_pipeline_stage_destination_mqtt_ToErrorMqttClientDTarget"
	topicResolutionError = "topic expression '%s' generated a null or empty topic"
)

type MqttClientDestination struct {
	*common.BaseStage
	*mqttlib.MqttConnector
	CommonConf          mqttlib.MqttClientConfigBean `ConfigDefBean:"commonConf"`
	PublisherConf       MqttClientTargetConfigBean   `ConfigDefBean:"publisherConf"`
	recordWriterFactory recordio.RecordWriterFactory
}

type MqttClientTargetConfigBean struct {
	TopicWhiteList            string                                  `ConfigDef:"type=STRING,required=true"`
	TopicExpression           string                                  `ConfigDef:"type=STRING,required=true,evaluation=EXPLICIT"`
	RuntimeTopicResolution    bool                                    `ConfigDef:"type=BOOLEAN,required=true"`
	Topic                     string                                  `ConfigDef:"type=STRING,required=true"`
	DataFormat                string                                  `ConfigDef:"type=STRING,required=true"`
	DataGeneratorFormatConfig datagenerator.DataGeneratorFormatConfig `ConfigDefBean:"dataGeneratorFormatConfig"`
}

func init() {
	stagelibrary.SetCreator(Library, StageName, func() api.Stage {
		return &MqttClientDestination{BaseStage: &common.BaseStage{}, MqttConnector: &mqttlib.MqttConnector{}}
	})

	stagelibrary.SetCreator(Library, ErrorStageName, func() api.Stage {
		return &MqttClientDestination{BaseStage: &common.BaseStage{}, MqttConnector: &mqttlib.MqttConnector{}}
	})
}

func (md *MqttClientDestination) Init(stageContext api.StageContext) []validation.Issue {
	log.Debug("MqttClientDestination Init method")
	issues := md.BaseStage.Init(stageContext)
	if err := md.InitializeClient(md.CommonConf); err != nil {
		issues = append(issues, stageContext.CreateConfigIssue(err.Error()))
		return issues
	}
	if md.GetStageContext().IsErrorStage() {
		md.PublisherConf.DataFormat = "SDC_JSON"
	}
	return md.PublisherConf.DataGeneratorFormatConfig.Init(md.PublisherConf.DataFormat, stageContext, issues)
}

func (md *MqttClientDestination) Write(batch api.Batch) error {
	log.Debug("MqttClientDestination write method")

	for _, record := range batch.GetRecords() {
		recordValueBuffer := bytes.NewBuffer([]byte{})
		if recordWriter, err := md.PublisherConf.DataGeneratorFormatConfig.RecordWriterFactory.CreateWriter(md.GetStageContext(), recordValueBuffer); err == nil {

			if err = recordWriter.WriteRecord(record); err != nil {
				log.WithError(err).Error("Error Writing Record")
				md.GetStageContext().ToError(err, record)
				continue
			}

			flushAndCloseWriter(recordWriter)

			if topic, err := md.resolveTopic(record); err != nil {
				log.WithError(err).Error("Error Writing Record")
				md.GetStageContext().ToError(err, record)
			} else {
				if tkn := md.Client.Publish(
					topic,
					byte(md.Qos),
					false,
					recordValueBuffer.Bytes(),
				); tkn.Wait() && tkn.Error() != nil {
					err = tkn.Error()
				}
			}

		} else {
			md.sendRecordsToError(batch.GetRecords(), err)
		}
	}

	return nil
}

func (md *MqttClientDestination) resolveTopic(record api.Record) (string, error) {
	if !md.PublisherConf.RuntimeTopicResolution {
		return md.PublisherConf.Topic, nil
	}

	recordContext := context.WithValue(context.Background(), el.RecordContextVar, record)
	result, err := md.GetStageContext().Evaluate(md.PublisherConf.TopicExpression, "topicExpression", recordContext)
	if err != nil {
		return "", err
	}

	if result == nil || cast.ToString(result) == "" {
		return "", fmt.Errorf(topicResolutionError, md.PublisherConf.TopicExpression)
	}

	topic := cast.ToString(result)
	return topic, nil
}

func (md *MqttClientDestination) sendRecordsToError(records []api.Record, err error) {
	log.WithError(err).Error("Error Writing records to destination")
	for _, record := range records {
		md.GetStageContext().ToError(err, record)
	}
}

func (md *MqttClientDestination) Destroy() error {
	log.Debug("MqttClientDestination Destroy method")
	md.Client.Disconnect(250)
	return nil
}

func flushAndCloseWriter(recordWriter dataformats.RecordWriter) {
	err := recordWriter.Flush()
	if err != nil {
		log.WithError(err).Error("Error flushing record writer")
	}

	err = recordWriter.Close()
	if err != nil {
		log.WithError(err).Error("Error closing record writer")
	}
}

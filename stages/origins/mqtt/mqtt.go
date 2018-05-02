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
package mqtt

import (
	"bytes"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/lib/dataparser"
	mqttlib "github.com/streamsets/datacollector-edge/stages/lib/mqtt"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
)

const (
	LIBRARY           = "streamsets-datacollector-basic-lib"
	STAGE_NAME        = "com_streamsets_pipeline_stage_origin_mqtt_MqttClientDSource"
	TOPIC_HEADER_NAME = "topic"
)

var stringOffset string = "mqtt-subscriber-offset"

type MqttClientSource struct {
	*common.BaseStage
	*mqttlib.MqttConnector
	CommonConf      mqttlib.MqttClientConfigBean `ConfigDefBean:"commonConf"`
	SubscriberConf  MqttClientSourceConfigBean   `ConfigDefBean:"subscriberConf"`
	incomingRecords chan api.Record
}

type MqttClientSourceConfigBean struct {
	TopicFilters     []string                          `ConfigDef:"type=LIST,required=true"`
	DataFormat       string                            `ConfigDef:"type=STRING,required=true"`
	DataFormatConfig dataparser.DataParserFormatConfig `ConfigDefBean:"dataFormatConfig"`
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &MqttClientSource{BaseStage: &common.BaseStage{}, MqttConnector: &mqttlib.MqttConnector{}}
	})
}

func (ms *MqttClientSource) getTopicFilterAndQosMap() map[string]byte {
	topicFilters := make(map[string]byte, len(ms.SubscriberConf.TopicFilters))
	for _, topicFilter := range ms.SubscriberConf.TopicFilters {
		topicFilters[topicFilter] = byte(ms.Qos)
	}
	return topicFilters
}

func (ms *MqttClientSource) Init(stageContext api.StageContext) []validation.Issue {
	log.Debug("MqttClientSource Init method")
	issues := ms.BaseStage.Init(stageContext)

	ms.incomingRecords = make(chan api.Record)

	err := ms.InitializeClient(ms.CommonConf)
	if err == nil {
		if token := ms.Client.SubscribeMultiple(
			ms.getTopicFilterAndQosMap(),
			ms.MessageHandler,
		); token.Wait() && token.Error() != nil {
			err = token.Error()
		}
	}
	return ms.SubscriberConf.DataFormatConfig.Init(ms.SubscriberConf.DataFormat, stageContext, issues)
}

func (ms *MqttClientSource) Produce(
	lastSourceOffset *string,
	maxBatchSize int,
	batchMaker api.BatchMaker,
) (*string, error) {
	log.Debug("MqttClientSource - Produce method")
	record := <-ms.incomingRecords
	batchMaker.AddRecord(record)
	return &stringOffset, nil
}

func (ms *MqttClientSource) Destroy() error {
	log.Debug("MqttClientSource - Destroy method")
	ms.Client.Unsubscribe(ms.SubscriberConf.TopicFilters...).Wait()
	ms.Client.Disconnect(250)
	// Close channel after unsubscribe and disconnect
	close(ms.incomingRecords)
	return nil
}

func (ms *MqttClientSource) MessageHandler(client MQTT.Client, msg MQTT.Message) {
	recordReaderFactory := ms.SubscriberConf.DataFormatConfig.RecordReaderFactory
	recordBuffer := bytes.NewBufferString(string(msg.Payload()))
	recordReader, err := recordReaderFactory.CreateReader(ms.GetStageContext(), recordBuffer)
	if err != nil {
		log.WithError(err).Error("Failed to create record reader")
	}
	defer recordReader.Close()

	for {
		record, err := recordReader.ReadRecord()
		if err != nil {
			log.WithError(err).Error("Failed to parse raw data")
		}

		if record == nil {
			break
		}
		record.GetHeader().SetAttribute(TOPIC_HEADER_NAME, msg.Topic())
		ms.incomingRecords <- record
	}
}

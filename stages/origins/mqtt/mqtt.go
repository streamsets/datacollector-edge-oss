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
	MQTT "github.com/eclipse/paho.mqtt.golang"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/lib/dataparser"
	mqttlib "github.com/streamsets/datacollector-edge/stages/lib/mqtt"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"time"
)

const (
	Library         = "streamsets-datacollector-basic-lib"
	StageName       = "com_streamsets_pipeline_stage_origin_mqtt_MqttClientDSource"
	TopicHeaderName = "topic"
)

var defaultOffset = "mqtt-subscriber-offset"

type Origin struct {
	*common.BaseStage
	*mqttlib.MqttConnector
	CommonConf      mqttlib.MqttClientConfigBean `ConfigDefBean:"commonConf"`
	SubscriberConf  SubscriberConfigBean         `ConfigDefBean:"subscriberConf"`
	incomingRecords chan api.Record
}

type SubscriberConfigBean struct {
	TopicFilters     []string                          `ConfigDef:"type=LIST,required=true"`
	DataFormat       string                            `ConfigDef:"type=STRING,required=true"`
	DataFormatConfig dataparser.DataParserFormatConfig `ConfigDefBean:"dataFormatConfig"`
}

func init() {
	stagelibrary.SetCreator(Library, StageName, func() api.Stage {
		return &Origin{BaseStage: &common.BaseStage{}, MqttConnector: &mqttlib.MqttConnector{}}
	})
}

func (ms *Origin) getTopicFilterAndQosMap() map[string]byte {
	topicFilters := make(map[string]byte, len(ms.SubscriberConf.TopicFilters))
	for _, topicFilter := range ms.SubscriberConf.TopicFilters {
		topicFilters[topicFilter] = byte(ms.Qos)
	}
	return topicFilters
}

func (ms *Origin) Init(stageContext api.StageContext) []validation.Issue {
	log.Debug("MQTT Subscriber Init method")
	issues := ms.BaseStage.Init(stageContext)

	ms.incomingRecords = make(chan api.Record)

	if err := ms.InitializeClient(ms.CommonConf); err != nil {
		issues = append(issues, stageContext.CreateConfigIssue(err.Error()))
		return issues
	}

	issues = ms.SubscriberConf.DataFormatConfig.Init(ms.SubscriberConf.DataFormat, stageContext, issues)
	if len(issues) > 0 {
		return issues
	}

	if token := ms.Client.SubscribeMultiple(
		ms.getTopicFilterAndQosMap(),
		ms.MessageHandler,
	); token.Wait() && token.Error() != nil {
		issues = append(issues, stageContext.CreateConfigIssue(token.Error().Error()))
		return issues
	}
	return issues
}

func (ms *Origin) Produce(
	lastSourceOffset *string,
	maxBatchSize int,
	batchMaker api.BatchMaker,
) (*string, error) {
	log.Debug("MQTT Subscriber - Produce method")
	timeout := time.NewTimer(time.Duration(5) * time.Second)
	defer timeout.Stop()
	end := false
	for !end && !ms.GetStageContext().IsStopped() {
		select {
		case record := <-ms.incomingRecords:
			if record != nil {
				batchMaker.AddRecord(record)
			}
			return &defaultOffset, nil
		case <-timeout.C:
			end = true
		}
	}
	return &defaultOffset, nil
}

func (ms *Origin) Destroy() error {
	log.Debug("MQTT Subscriber - Destroy method")
	ms.Client.Unsubscribe(ms.SubscriberConf.TopicFilters...).Wait()
	ms.Client.Disconnect(250)
	// Close channel after unsubscribe and disconnect
	close(ms.incomingRecords)
	return nil
}

func (ms *Origin) MessageHandler(client MQTT.Client, msg MQTT.Message) {
	recordReaderFactory := ms.SubscriberConf.DataFormatConfig.RecordReaderFactory
	recordBuffer := bytes.NewBufferString(string(msg.Payload()))
	recordReader, err := recordReaderFactory.CreateReader(ms.GetStageContext(), recordBuffer, "mqtt")
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
		record.GetHeader().SetAttribute(TopicHeaderName, msg.Topic())
		ms.incomingRecords <- record
	}
}

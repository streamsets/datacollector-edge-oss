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
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"testing"
)

func getStageContext(
	brokerUrl string,
	clientId string,
	qos string,
	topicFilters []string,
	dataFormat string,
	parameters map[string]interface{},
) *common.StageContextImpl {
	stageConfig := common.StageConfiguration{}
	stageConfig.Library = LIBRARY
	stageConfig.StageName = STAGE_NAME
	stageConfig.Configuration = []common.Config{
		{
			Name:  "commonConf.brokerUrl",
			Value: brokerUrl,
		},
		{
			Name:  "commonConf.clientId",
			Value: clientId,
		},
		{
			Name:  "commonConf.qos",
			Value: qos,
		},
		{
			Name:  "subscriberConf.topicFilters",
			Value: topicFilters,
		},
		{
			Name:  "subscriberConf.dataFormat",
			Value: dataFormat,
		},
	}
	return &common.StageContextImpl{
		StageConfig: &stageConfig,
		Parameters:  parameters,
	}
}

func TestMqttClientSource_Init(t *testing.T) {
	brokerUrl := "http://test:9000"
	clientId := "clientId"
	qos := "AT_LEAST_ONCE"
	topicFilters := []string{"Sample/Topic"}
	dataFormat := "JSON"

	stageContext := getStageContext(brokerUrl, clientId, qos, topicFilters, dataFormat, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters)
	if err != nil {
		t.Error(err)
		return
	}

	stageInstance := stageBean.Stage
	if stageInstance == nil {
		t.Error("Failed to create stage instance")
	}

	if stageInstance.(*MqttClientSource).CommonConf.BrokerUrl != brokerUrl {
		t.Error("Failed to inject config value for brokerUrl")
	}

	if stageInstance.(*MqttClientSource).CommonConf.ClientId != clientId {
		t.Error("Failed to inject config value for clientId")
	}

	if stageInstance.(*MqttClientSource).CommonConf.Qos != qos {
		t.Error("Failed to inject config value for qos")
	}

	if len(stageInstance.(*MqttClientSource).SubscriberConf.TopicFilters) != len(topicFilters) {
		t.Error("Failed to inject config value for topicFilters")
		return
	}

	if stageInstance.(*MqttClientSource).SubscriberConf.TopicFilters[0] != topicFilters[0] {
		t.Error("Failed to inject config value for topicFilters")
		return
	}

	if stageInstance.(*MqttClientSource).SubscriberConf.DataFormat != dataFormat {
		t.Error("Failed to inject config value for dataFormat")
	}
}

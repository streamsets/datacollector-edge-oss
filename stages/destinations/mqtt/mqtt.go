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
	"encoding/json"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	mqttlib "github.com/streamsets/datacollector-edge/stages/lib/mqtt"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"log"
)

const (
	LIBRARY    = "streamsets-datacollector-basic-lib"
	STAGE_NAME = "com_streamsets_pipeline_stage_destination_mqtt_MqttClientDTarget"
)

type MqttClientDestination struct {
	*common.BaseStage
	*mqttlib.MqttConnector
	CommonConf    mqttlib.MqttClientConfigBean `ConfigDefBean:"commonConf"`
	PublisherConf MqttClientTargetConfigBean   `ConfigDefBean:"publisherConf"`
}

type MqttClientTargetConfigBean struct {
	Topic      string `ConfigDef:"type=STRING,required=true"`
	DataFormat string `ConfigDef:"type=STRING,required=true"`
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &MqttClientDestination{BaseStage: &common.BaseStage{}, MqttConnector: &mqttlib.MqttConnector{}}
	})
}

func (md *MqttClientDestination) Init(stageContext api.StageContext) error {
	log.Println("[DEBUG] MqttClientDestination Init method")
	if err := md.BaseStage.Init(stageContext); err != nil {
		return err
	}
	return md.InitializeClient(md.CommonConf)
}

func (md *MqttClientDestination) Write(batch api.Batch) error {
	log.Println("[DEBUG] MqttClientDestination write method")
	for _, record := range batch.GetRecords() {
		recordValue, _ := record.Get()
		err := md.sendRecordToSDC(recordValue.Value)
		if err != nil {
			log.Println("[Error] Error Writing Record", err)
			md.GetStageContext().ToError(err, record)
		}
	}
	return nil
}

func (md *MqttClientDestination) sendRecordToSDC(recordValue interface{}) error {
	var err error = nil
	if jsonValue, e := json.Marshal(recordValue); e == nil {
		if token := md.Client.Publish(md.PublisherConf.Topic, byte(md.Qos), false, jsonValue); token.Wait() && token.Error() != nil {
			err = token.Error()
		}
	}
	return err
}

func (md *MqttClientDestination) Destroy() error {
	log.Println("[DEBUG] MqttClientDestination Destroy method")
	md.Client.Disconnect(250)
	return nil
}

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
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/recordio"
	"github.com/streamsets/datacollector-edge/stages/lib/datagenerator"
	mqttlib "github.com/streamsets/datacollector-edge/stages/lib/mqtt"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"log"
)

const (
	LIBRARY          = "streamsets-datacollector-basic-lib"
	STAGE_NAME       = "com_streamsets_pipeline_stage_destination_mqtt_MqttClientDTarget"
	ERROR_STAGE_NAME = "com_streamsets_pipeline_stage_destination_mqtt_ToErrorMqttClientDTarget"
)

type MqttClientDestination struct {
	*common.BaseStage
	*mqttlib.MqttConnector
	CommonConf          mqttlib.MqttClientConfigBean `ConfigDefBean:"commonConf"`
	PublisherConf       MqttClientTargetConfigBean   `ConfigDefBean:"publisherConf"`
	recordWriterFactory recordio.RecordWriterFactory
}

type MqttClientTargetConfigBean struct {
	Topic                     string                                  `ConfigDef:"type=STRING,required=true"`
	DataFormat                string                                  `ConfigDef:"type=STRING,required=true"`
	DataGeneratorFormatConfig datagenerator.DataGeneratorFormatConfig `ConfigDefBean:"dataGeneratorFormatConfig"`
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &MqttClientDestination{BaseStage: &common.BaseStage{}, MqttConnector: &mqttlib.MqttConnector{}}
	})

	stagelibrary.SetCreator(LIBRARY, ERROR_STAGE_NAME, func() api.Stage {
		return &MqttClientDestination{BaseStage: &common.BaseStage{}, MqttConnector: &mqttlib.MqttConnector{}}
	})
}

func (md *MqttClientDestination) Init(stageContext api.StageContext) error {
	log.Println("[DEBUG] MqttClientDestination Init method")
	if err := md.BaseStage.Init(stageContext); err != nil {
		return err
	}
	if err := md.InitializeClient(md.CommonConf); err != nil {
		return err
	}
	if md.GetStageContext().IsErrorStage() {
		md.PublisherConf.DataFormat = "SDC_JSON"
	}
	return md.PublisherConf.DataGeneratorFormatConfig.Init(md.PublisherConf.DataFormat)
}

func (md *MqttClientDestination) Write(batch api.Batch) error {
	log.Println("[DEBUG] MqttClientDestination write method")
	for _, record := range batch.GetRecords() {
		err := md.sendRecordToSDC(record)
		if err != nil {
			log.Println("[Error] Error Writing Record", err)
			md.GetStageContext().ToError(err, record)
		}
	}
	return nil
}

func (md *MqttClientDestination) sendRecordToSDC(record api.Record) error {
	var err error = nil
	var recordWriter recordio.RecordWriter
	recordValueBuffer := bytes.NewBuffer([]byte{})
	if recordWriter, err = md.PublisherConf.DataGeneratorFormatConfig.RecordWriterFactory.CreateWriter(md.GetStageContext(), recordValueBuffer); err == nil {
		if err = recordWriter.WriteRecord(record); err == nil {
			if err = recordWriter.Close(); err == nil {
				if token := md.Client.Publish(md.PublisherConf.Topic, byte(md.Qos), false, recordValueBuffer.Bytes()); token.Wait() && token.Error() != nil {
					err = token.Error()
				}
			}
		}
	}
	return err
}

func (md *MqttClientDestination) Destroy() error {
	log.Println("[DEBUG] MqttClientDestination Destroy method")
	md.Client.Disconnect(250)
	return nil
}

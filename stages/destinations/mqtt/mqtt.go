package mqtt

import (
	"context"
	"encoding/json"
	"github.com/streamsets/dataextractor/api"
	"github.com/streamsets/dataextractor/container/common"
	mqttlib "github.com/streamsets/dataextractor/stages/lib/mqtt"
	"github.com/streamsets/dataextractor/stages/stagelibrary"
	"log"
)

const (
	LIBRARY    = "streamsets-datacollector-basic-lib"
	STAGE_NAME = "com_streamsets_pipeline_stage_destination_mqtt_MqttClientDTarget"
)

type MqttClientDestination struct {
	mqttlib.MqttConnector
	topic string
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &MqttClientDestination{}
	})
}

func (md *MqttClientDestination) Init(ctx context.Context) error {
	stageContext := common.GetStageContext(ctx)
	stageConfig := stageContext.StageConfig
	log.Println("[DEBUG] MqttClientDestination Init method")

	md.MqttConnector = mqttlib.MqttConnector{}

	for _, config := range stageConfig.Configuration {
		configName, configValue := config.Name, stageContext.GetResolvedValue(config.Value)
		if configName == "publisherConf.topic" {
			md.topic = configValue.(string)
		} else {
			md.InitConfig(configName, configValue)
		}
	}

	return md.InitializeClient()
}

func (md *MqttClientDestination) Write(batch api.Batch) error {
	log.Println("[DEBUG] MqttClientDestination write method")
	for _, record := range batch.GetRecords() {
		md.sendRecordToSDC(record.GetValue())
	}
	return nil
}

func (md *MqttClientDestination) sendRecordToSDC(recordValue interface{}) {
	if jsonValue, err := json.Marshal(recordValue); err == nil {
		if token := md.Client.Publish(md.topic, byte(md.Qos), false, jsonValue); token.Wait() && token.Error() != nil {
			panic(token.Error())
		}
	} else {
		panic(err)
	}
}

func (md *MqttClientDestination) Destroy() error {
	log.Println("[DEBUG] MqttClientDestination Destroy method")
	md.Client.Disconnect(250)
	return nil
}

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

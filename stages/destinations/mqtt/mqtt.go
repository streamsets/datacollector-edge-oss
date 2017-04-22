package mqtt

import (
	"context"
	"encoding/json"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/streamsets/dataextractor/api"
	"github.com/streamsets/dataextractor/container/common"
	"github.com/streamsets/dataextractor/stages/stagelibrary"
	"log"
)

const (
	LIBRARY    = "streamsets-datacollector-basic-lib"
	STAGE_NAME = "com_streamsets_pipeline_stage_destination_mqtt_MqttClientDTarget"
)

type MqttClientDestination struct {
	brokerUrl string
	clientId  string
	topic     string
	qos       float64
	opts      *MQTT.ClientOptions
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &MqttClientDestination{}
	})
}

func (m *MqttClientDestination) Init(ctx context.Context) {
	stageContext := (ctx.Value("stageContext")).(common.StageContext)
	stageConfig := stageContext.StageConfig
	log.Println("MqttClientDestination Init method")
	for _, config := range stageConfig.Configuration {
		if config.Name == "conf.brokerUrl" {
			m.brokerUrl = config.Value.(string)
		}

		if config.Name == "conf.clientId" {
			m.clientId = config.Value.(string)
		}

		if config.Name == "conf.topic" {
			m.topic = config.Value.(string)
		}

		if config.Name == "conf.qos" {
			m.qos = config.Value.(float64)

		}
	}

	m.opts = MQTT.NewClientOptions().AddBroker(m.brokerUrl)
	m.opts.SetClientID(m.clientId)
	m.opts.SetDefaultPublishHandler(m.MessageHandler)
}

func (m *MqttClientDestination) Write(batch api.Batch) error {
	//create and start a client using the above ClientOptions
	client := MQTT.NewClient(m.opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	log.Println("MqttClientDestination write method")
	for _, record := range batch.GetRecords() {
		m.sendRecordToSDC(record.Value, client)
	}
	client.Disconnect(250)
	return nil
}

func (h *MqttClientDestination) sendRecordToSDC(recordValue interface{}, client MQTT.Client) {
	jsonValue, err := json.Marshal(recordValue)
	if err != nil {
		panic(err)
	}

	token := client.Publish(h.topic, byte(h.qos), false, jsonValue)
	token.Wait()
}

//define a function for the default message handler
func (m *MqttClientDestination) MessageHandler(client MQTT.Client, msg MQTT.Message) {
	log.Printf("TOPIC: %s\n", msg.Topic())
	log.Printf("MSG: %s\n", msg.Payload())
}

func (h *MqttClientDestination) Destroy() {

}

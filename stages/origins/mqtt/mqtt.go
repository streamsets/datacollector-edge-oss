package mqtt

import (
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	mqttlib "github.com/streamsets/datacollector-edge/stages/lib/mqtt"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"log"
	"strconv"
)

const (
	LIBRARY    = "streamsets-datacollector-basic-lib"
	STAGE_NAME = "com_streamsets_pipeline_stage_origin_mqtt_MqttClientDSource"
)

type MqttClientSource struct {
	*common.BaseStage
	*mqttlib.MqttConnector
	CommonConf      mqttlib.MqttClientConfigBean `ConfigDefBean:"commonConf"`
	SubscriberConf  MqttClientSourceConfigBean   `ConfigDefBean:"subscriberConf"`
	incomingRecords chan api.Record
}

type MqttClientSourceConfigBean struct {
	TopicFilters []string `ConfigDef:"type=LIST,required=true"`
	DataFormat   string   `ConfigDef:"type=STRING,required=true"`
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &MqttClientSource{BaseStage: &common.BaseStage{}, MqttConnector: &mqttlib.MqttConnector{}}
	})
}

func (ms *MqttClientSource) getTopicFilterAndQosMap() map[string]byte {
	topicFilters := make(map[string]byte, len(ms.SubscriberConf.TopicFilters))
	for _, topicFilter := range ms.SubscriberConf.TopicFilters {
		topicFilters[topicFilter] = byte(ms.CommonConf.Qos)
	}
	return topicFilters
}

func (ms *MqttClientSource) Init(stageContext api.StageContext) error {
	log.Println("[DEBUG] MqttClientSource Init method")
	if err := ms.BaseStage.Init(stageContext); err != nil {
		return err
	}

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
	return err
}

func (ms *MqttClientSource) Produce(
	lastSourceOffset string,
	maxBatchSize int,
	batchMaker api.BatchMaker,
) (string, error) {
	log.Println("[DEBUG] MqttClientSource - Produce method")
	record := <-ms.incomingRecords
	batchMaker.AddRecord(record)
	return "", nil
}

func (ms *MqttClientSource) Destroy() error {
	log.Println("[DEBUG] MqttClientSource - Destroy method")
	ms.Client.Unsubscribe(ms.SubscriberConf.TopicFilters...).Wait()
	ms.Client.Disconnect(250)
	//Close channel after unsubscribe and disconnect
	close(ms.incomingRecords)
	return nil
}

func (md *MqttClientSource) MessageHandler(client MQTT.Client, msg MQTT.Message) {
	value := string(msg.Payload())
	msgId := strconv.FormatUint(uint64(msg.MessageID()), 10)
	log.Println("[DEBUG] Incoming Data: ", value)
	record, _ := md.GetStageContext().CreateRecord(msgId, value)
	md.incomingRecords <- record
}

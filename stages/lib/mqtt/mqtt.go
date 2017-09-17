package mqtt

import (
	MQTT "github.com/eclipse/paho.mqtt.golang"
)

type MqttClientConfigBean struct {
	BrokerUrl string  `ConfigDef:"type=STRING,required=true"`
	ClientId  string  `ConfigDef:"type=STRING,required=true"`
	Qos       float64 `ConfigDef:"type=NUMBER,required=true"`
}

type MqttConnector struct {
	Client MQTT.Client
}

func (m *MqttConnector) InitializeClient(commonConf MqttClientConfigBean) error {
	opts := MQTT.NewClientOptions().AddBroker(commonConf.BrokerUrl).SetClientID(commonConf.ClientId)
	m.Client = MQTT.NewClient(opts)
	if token := m.Client.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

package mqtt

import (
	MQTT "github.com/eclipse/paho.mqtt.golang"
)

type MqttConnector struct {
	BrokerUrl string
	ClientId  string
	Qos       float64
	Client    MQTT.Client
}

func (m *MqttConnector) InitConfig(configName string, configValue interface{}) {
	switch configName {
	case "commonConf.brokerUrl":
		m.BrokerUrl = configValue.(string)
	case "commonConf.clientId":
		m.ClientId = configValue.(string)
	case "commonConf.qos":
		m.Qos = configValue.(float64)
	}
}

func (m *MqttConnector) InitializeClient() error {
	opts := MQTT.NewClientOptions().AddBroker(m.BrokerUrl).SetClientID(m.ClientId)
	m.Client = MQTT.NewClient(opts)
	if token := m.Client.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

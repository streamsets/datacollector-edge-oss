package mqtt

import (
	"errors"
	MQTT "github.com/eclipse/paho.mqtt.golang"
)

const (
	AtMostOnce  = "AT_MOST_ONCE"
	AtLeastOnce = "AT_LEAST_ONCE"
	ExactlyOnce = "EXACTLY_ONCE"
)

type MqttClientConfigBean struct {
	BrokerUrl string `ConfigDef:"type=STRING,required=true"`
	ClientId  string `ConfigDef:"type=STRING,required=true"`
	Qos       string `ConfigDef:"type=STRING,required=true"`
}

type MqttConnector struct {
	Client MQTT.Client
	Qos    float64
}

func (m *MqttConnector) InitializeClient(commonConf MqttClientConfigBean) error {
	var err error
	if m.Qos, err = m.getQosFromString(commonConf.Qos); err != nil {
		return err
	}
	opts := MQTT.NewClientOptions().AddBroker(commonConf.BrokerUrl).SetClientID(commonConf.ClientId)
	m.Client = MQTT.NewClient(opts)
	if token := m.Client.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

func (m *MqttConnector) getQosFromString(qosString string) (float64, error) {
	switch qosString {
	case AtMostOnce:
		return float64(0), nil
	case AtLeastOnce:
		return float64(1), nil
	case ExactlyOnce:
		return float64(2), nil
	default:
		return float64(-1), errors.New("Unsupported Qos : " + qosString)
	}
}

// Copyright 2018 StreamSets Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
	UseAuth   bool   `ConfigDef:"type=BOOLEAN,required=true"`
	Username  string `ConfigDef:"type=STRING,required=true"`
	Password  string `ConfigDef:"type=STRING,required=true"`
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

	if commonConf.UseAuth {
		opts.SetUsername(commonConf.Username)
		opts.SetPassword(commonConf.Password)
	}

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

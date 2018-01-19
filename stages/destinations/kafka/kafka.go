// +build kafka

/*
 * Copyright 2018 StreamSets Inc.
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
package kafka

import (
	"bytes"
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/el"
	"github.com/streamsets/datacollector-edge/stages/lib/datagenerator"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
)

const (
	LIBRARY           = "streamsets-datacollector-apache-kafka_1_0-lib"
	STAGE_NAME        = "com_streamsets_pipeline_stage_destination_kafka_KafkaDTarget"
	BOOTSTRAP_SERVERS = "bootstrap.servers"
)

type KafkaDestination struct {
	*common.BaseStage
	Producer *kafka.Producer
	Conf     KafkaTargetConfig `ConfigDefBean:"conf"`
}

type KafkaTargetConfig struct {
	MetadataBrokerList        string                                  `ConfigDef:"type=STRING,required=true"`
	TopicWhiteList            string                                  `ConfigDef:"type=STRING,required=true"`
	Topic                     string                                  `ConfigDef:"type=STRING,required=true"`
	TopicExpression           string                                  `ConfigDef:"type=STRING,required=true,evaluation=EXPLICIT"`
	RuntimeTopicResolution    bool                                    `ConfigDef:"type=BOOLEAN,required=true"`
	PartitionStrategy         PartitionStrategy                       `ConfigDef:"type=MODEL,required=true"`
	SingleMessagePerBatch     bool                                    `ConfigDef:"type=BOOLEAN,required=true"`
	KafkaProducerConfigs      map[string]string                       `ConfigDef:"type=MAP,required=true"`
	DataFormat                string                                  `ConfigDef:"type=STRING,required=true"`
	DataGeneratorFormatConfig datagenerator.DataGeneratorFormatConfig `ConfigDefBean:"dataGeneratorFormatConfig"`
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &KafkaDestination{BaseStage: &common.BaseStage{}}
	})
}

func (dest *KafkaDestination) Init(context api.StageContext) error {
	var err error
	if err = dest.BaseStage.Init(context); err != nil {
		return err
	}
	if err = dest.Conf.DataGeneratorFormatConfig.Init(dest.Conf.DataFormat); err != nil {
		return err
	}

	kafkaConfigMap := kafka.ConfigMap{
		BOOTSTRAP_SERVERS: dest.Conf.MetadataBrokerList,
	}

	for key, value := range dest.Conf.KafkaProducerConfigs {
		kafkaConfigMap[key] = value
	}

	producer, err := kafka.NewProducer(&kafkaConfigMap)

	if err != nil {
		return err
	}

	dest.Producer = producer

	return nil
}

func (dest *KafkaDestination) Write(batch api.Batch) error {
	var err error
	recordWriterFactory := dest.Conf.DataGeneratorFormatConfig.RecordWriterFactory

	if err != nil {
		return err
	}

	doneChan := make(chan bool)

	go func() {
		defer close(doneChan)
		for e := range dest.Producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				msgLogger := log.WithFields(log.Fields{
					"key":       m.Key,
					"topic":     m.TopicPartition.Topic,
					"partition": m.TopicPartition.Partition,
					"offset":    m.TopicPartition.Offset,
				})
				if m.TopicPartition.Error != nil {
					msgLogger.WithError(m.TopicPartition.Error).Error("Message delivery failed!")
				} else {
					msgLogger.Debug("Message delivered")
				}
				return

			default:
				log.WithField("event", ev).Warn("Ignored event")
			}
		}
	}()

	// TODO: Support sending single message per batch -
	// SDCE-176 - Support sending sing message per batch in Kafka destination

	for _, record := range batch.GetRecords() {
		recordContext := context.WithValue(context.Background(), el.RECORD_CONTEXT_VAR, record)

		recordBuffer := bytes.NewBuffer([]byte{})
		recordWriter, err := recordWriterFactory.CreateWriter(dest.GetStageContext(), recordBuffer)

		err = recordWriter.WriteRecord(record)
		if err != nil {
			return err
		}

		err = recordWriter.Flush()
		if err != nil {
			log.WithError(err).Error("Error flushing record writer")
		}

		err = recordWriter.Close()
		if err != nil {
			log.WithError(err).Error("Error closing record writer")
		}

		topic, err := resolveTopic(dest.GetStageContext(), recordContext, &dest.Conf)
		if err != nil {
			return err
		}

		p, err := nextPartition(dest.Producer, topic, dest.Conf.PartitionStrategy)
		if err != nil {
			return err
		}

		log.WithField("partition", p).Debug("Sending message")

		dest.Producer.ProduceChannel() <- &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic: topic, Partition: p,
			},
			Value: recordBuffer.Bytes(),
		}
	}

	// wait for delivery report
	_ = <-doneChan
	return nil
}

func (dest *KafkaDestination) Destroy() error {
	if dest.Producer != nil {
		dest.Producer.Close()
	}
	return nil
}

func resolveTopic(stageContext api.StageContext, recordContext context.Context, config *KafkaTargetConfig) (*string, error) {
	if !config.RuntimeTopicResolution {
		return &config.Topic, nil
	}

	result, err := stageContext.Evaluate(config.TopicExpression, "topicExpression", recordContext)
	if err != nil {
		return nil, err
	}

	topic := result.(string)
	return &topic, nil
}

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
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/el"
	"github.com/streamsets/datacollector-edge/stages/lib/datagenerator"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"strings"
)

const (
	LIBRARY    = "streamsets-datacollector-apache-kafka_1_0-lib"
	STAGE_NAME = "com_streamsets_pipeline_stage_destination_kafka_KafkaDTarget"
)

type KafkaDestination struct {
	*common.BaseStage
	Conf            KafkaTargetConfig `ConfigDefBean:"conf"`
	kafkaClientConf *sarama.Config
	brokerList      []string
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

	dest.kafkaClientConf = sarama.NewConfig()
	dest.kafkaClientConf.Producer.Return.Successes = true
	// TODO: Map KafkaProducerConfigs to sarama producer config
	dest.kafkaClientConf.Producer.Partitioner, err = getPartitionerConstructor(dest.Conf.PartitionStrategy)
	if err != nil {
		return err
	}
	dest.brokerList = strings.Split(dest.Conf.MetadataBrokerList, ",")

	return nil
}

func (dest *KafkaDestination) Write(batch api.Batch) error {
	var err error

	kafkaProducer, err := sarama.NewAsyncProducer(dest.brokerList, dest.kafkaClientConf)
	if err != nil {
		return err
	}

	defer func() {
		if err := kafkaProducer.Close(); err != nil {
			log.WithError(err).Error("Failed to close Kafka Producer")
		}
	}()

	recordWriterFactory := dest.Conf.DataGeneratorFormatConfig.RecordWriterFactory
	if err != nil {
		return err
	}

	go func() {
		for msg := range kafkaProducer.Successes() {
			log.WithFields(log.Fields{
				"key":       msg.Key,
				"topic":     msg.Topic,
				"partition": msg.Partition,
				"offset":    msg.Offset,
			}).Debug("Message delivered")
		}
	}()

	go func() {
		for err := range kafkaProducer.Errors() {
			log.WithFields(log.Fields{
				"key":       err.Msg.Key,
				"topic":     err.Msg.Topic,
				"partition": err.Msg.Partition,
				"offset":    err.Msg.Offset,
			}).WithError(err.Err).Error("Message delivery failed!")
		}
	}()

	// TODO: Support sending single message per batch -
	// SDCE-176 - Support sending single message per batch in Kafka destination

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

		log.Debug("Sending message")

		kafkaProducer.Input() <- &sarama.ProducerMessage{
			Topic: *topic,
			Value: sarama.ByteEncoder(recordBuffer.Bytes()),
		}
	}

	return nil
}

func (dest *KafkaDestination) Destroy() error {
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

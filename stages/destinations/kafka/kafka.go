// +build kafka

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
package kafka

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cast"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/dataformats"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/el"
	"github.com/streamsets/datacollector-edge/stages/lib/datagenerator"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"io/ioutil"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	APACHE_KAFKA_0_10_LIBRARY = "streamsets-datacollector-apache-kafka_0_10-lib"
	APACHE_KAFKA_0_11_LIBRARY = "streamsets-datacollector-apache-kafka_0_11-lib"
	APACHE_KAFKA_1_0_LIBRARY  = "streamsets-datacollector-apache-kafka_1_0-lib"
	APACHE_KAFKA_1_1_LIBRARY  = "streamsets-datacollector-apache-kafka_1_1-lib"
	APACHE_KAFKA_2_0_LIBRARY  = "streamsets-datacollector-apache-kafka_2_0-lib"

	CDH_KAFKA_2_0_LIBRARY = "streamsets-datacollector-cdh_kafka_2_0-lib"
	CDH_KAFKA_2_1_LIBRARY = "streamsets-datacollector-cdh_kafka_2_1-lib"
	CDH_KAFKA_3_0_LIBRARY = "streamsets-datacollector-cdh_kafka_3_0-lib"
	CDH_KAFKA_3_1_LIBRARY = "streamsets-datacollector-cdh_kafka_3_1-lib"
	CDH_6_0_LIBRARY       = "streamsets-datacollector-cdh_6_0-lib"

	HDP_KAFKA_2_4_LIBRARY = "streamsets-datacollector-hdp_2_4-lib"
	HDP_KAFKA_2_5_LIBRARY = "streamsets-datacollector-hdp_2_5-lib"
	HDP_KAFKA_2_6_LIBRARY = "streamsets-datacollector-hdp_2_6-lib"

	StageName = "com_streamsets_pipeline_stage_destination_kafka_KafkaDTarget"

	SocketTimeoutMS                    = "socket.timeout.ms"
	SslEndpointIdentificationAlgorithm = "ssl.endpoint.identification.algorithm"
	SecurityProtocol                   = "security.protocol"
	SASLJaasConfig                     = "sasl.jaas.config"
	SslTruststoreLocation              = "ssl.truststore.location"
	MessageMaxBytes                    = "message.max.bytes"
	RequestRequiredACKs                = "request.required.acks"
	RequestTimeoutMS                   = "request.timeout.ms"
	CompressionType                    = "compression.type"
	QueueBufferingMaxMS                = "queue.buffering.max.ms"
	QueueBufferingMaxMessages          = "queue.buffering.max.messages"
	MessageSendMaxRetries              = "message.send.max.retries"
	RetryBackoffMS                     = "retry.backoff.ms"

	ClientId = "SDCEdge"
	HTTPS    = "https"

	SecurityProtocolPlainText     = "PLAINTEXT"
	SecurityProtocolSSL           = "SSL"
	SecurityProtocolSASLPlainText = "SASL_PLAINTEXT"
	SecurityProtocolSASLSSL       = "SASL_SSL"

	InsecureSkipVerify = "insecureSkipVerify"

	SSSLJaasConfigRegex   = `.*username="(.*)".*password="(.*)"`
	CompressionTypeNone   = "none"
	CompressionTypeGzip   = "gzip"
	CompressionTypeSnappy = "snappy"
	CompressionTypeLz4    = "lz4"

	topicResolutionError = "topic expression '%s' generated a null or empty topic"
)

type KafkaDestination struct {
	*common.BaseStage
	Conf            KafkaTargetConfig `ConfigDefBean:"conf"`
	kafkaVersion    sarama.KafkaVersion
	kafkaClientConf *sarama.Config
	brokerList      []string
	kafkaClient     sarama.Client
	keyCounter      int
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
	stagelibrary.SetCreator(APACHE_KAFKA_0_10_LIBRARY, StageName, func() api.Stage {
		return &KafkaDestination{BaseStage: &common.BaseStage{}, kafkaVersion: sarama.V0_10_0_0}
	})
	stagelibrary.SetCreator(APACHE_KAFKA_0_11_LIBRARY, StageName, func() api.Stage {
		return &KafkaDestination{BaseStage: &common.BaseStage{}, kafkaVersion: sarama.V0_11_0_0}
	})
	stagelibrary.SetCreator(APACHE_KAFKA_1_0_LIBRARY, StageName, func() api.Stage {
		return &KafkaDestination{BaseStage: &common.BaseStage{}, kafkaVersion: sarama.V1_0_0_0}
	})
	stagelibrary.SetCreator(APACHE_KAFKA_1_1_LIBRARY, StageName, func() api.Stage {
		return &KafkaDestination{BaseStage: &common.BaseStage{}, kafkaVersion: sarama.V1_1_0_0}
	})
	stagelibrary.SetCreator(APACHE_KAFKA_2_0_LIBRARY, StageName, func() api.Stage {
		return &KafkaDestination{BaseStage: &common.BaseStage{}, kafkaVersion: sarama.V1_1_0_0}
	})

	stagelibrary.SetCreator(CDH_KAFKA_2_0_LIBRARY, StageName, func() api.Stage {
		return &KafkaDestination{BaseStage: &common.BaseStage{}, kafkaVersion: sarama.V0_9_0_0}
	})
	stagelibrary.SetCreator(CDH_KAFKA_2_1_LIBRARY, StageName, func() api.Stage {
		return &KafkaDestination{BaseStage: &common.BaseStage{}, kafkaVersion: sarama.V0_10_0_0}
	})
	stagelibrary.SetCreator(CDH_KAFKA_3_0_LIBRARY, StageName, func() api.Stage {
		return &KafkaDestination{BaseStage: &common.BaseStage{}, kafkaVersion: sarama.V0_11_0_0}
	})
	stagelibrary.SetCreator(CDH_KAFKA_3_1_LIBRARY, StageName, func() api.Stage {
		return &KafkaDestination{BaseStage: &common.BaseStage{}, kafkaVersion: sarama.V1_0_0_0}
	})
	stagelibrary.SetCreator(CDH_6_0_LIBRARY, StageName, func() api.Stage {
		return &KafkaDestination{BaseStage: &common.BaseStage{}, kafkaVersion: sarama.V1_1_0_0}
	})

	stagelibrary.SetCreator(HDP_KAFKA_2_4_LIBRARY, StageName, func() api.Stage {
		return &KafkaDestination{BaseStage: &common.BaseStage{}, kafkaVersion: sarama.V0_10_0_0}
	})
	stagelibrary.SetCreator(HDP_KAFKA_2_5_LIBRARY, StageName, func() api.Stage {
		return &KafkaDestination{BaseStage: &common.BaseStage{}, kafkaVersion: sarama.V0_10_0_0}
	})
	stagelibrary.SetCreator(HDP_KAFKA_2_6_LIBRARY, StageName, func() api.Stage {
		return &KafkaDestination{BaseStage: &common.BaseStage{}, kafkaVersion: sarama.V1_0_0_0}
	})
}

func (dest *KafkaDestination) Init(context api.StageContext) []validation.Issue {
	issues := dest.BaseStage.Init(context)
	issues = dest.Conf.DataGeneratorFormatConfig.Init(dest.Conf.DataFormat, context, issues)

	var err error

	err = dest.mapJVMConfigsToSaramaConfig()
	if err != nil {
		issues = append(issues, context.CreateConfigIssue(err.Error()))
		return issues
	}

	dest.kafkaClientConf.Producer.Partitioner, err = getPartitionerConstructor(dest.Conf.PartitionStrategy)
	if err != nil {
		issues = append(issues, context.CreateConfigIssue(err.Error()))
		return issues
	}
	dest.brokerList = strings.Split(dest.Conf.MetadataBrokerList, ",")

	dest.kafkaClient, err = sarama.NewClient(dest.brokerList, dest.kafkaClientConf)
	if err != nil {
		issues = append(issues, context.CreateConfigIssue(err.Error()))
		return issues
	}

	dest.keyCounter = 0
	return issues
}

func (dest *KafkaDestination) Write(batch api.Batch) error {
	if len(batch.GetRecords()) > 0 {
		var err error
		var wg sync.WaitGroup

		kafkaProducer, err := sarama.NewAsyncProducerFromClient(dest.kafkaClient)
		if err != nil {
			return err
		}

		defer func() {
			kafkaProducer.AsyncClose()
			wg.Wait()
		}()

		recordWriterFactory := dest.Conf.DataGeneratorFormatConfig.RecordWriterFactory
		if err != nil {
			return err
		}

		wg.Add(2)
		go func() {
			defer wg.Done()
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
			defer wg.Done()
			for err := range kafkaProducer.Errors() {
				log.WithFields(log.Fields{
					"key":       err.Msg.Key,
					"topic":     err.Msg.Topic,
					"partition": err.Msg.Partition,
					"offset":    err.Msg.Offset,
				}).WithError(err.Err).Error("Message delivery failed!")

				if dest.Conf.SingleMessagePerBatch {
					for _, record := range batch.GetRecords() {
						dest.GetStageContext().ToError(err, record)
					}
				} else {
					dest.GetStageContext().ToError(err, err.Msg.Metadata.(api.Record))
				}
			}
		}()

		if dest.Conf.SingleMessagePerBatch {

			topicToRecordsMap := make(map[*string][]api.Record)

			for _, record := range batch.GetRecords() {
				recordContext := context.WithValue(context.Background(), el.RecordContextVar, record)
				if topic, err := resolveTopic(dest.GetStageContext(), recordContext, &dest.Conf); err != nil {
					dest.GetStageContext().ToError(err, record)
					log.WithError(err).Error("resolve topic error")
				} else {
					if topicToRecordsMap[topic] == nil {
						topicToRecordsMap[topic] = make([]api.Record, 0)
					}
					topicToRecordsMap[topic] = append(topicToRecordsMap[topic], record)
				}
			}

			for topicName, records := range topicToRecordsMap {
				recordBuffer := bytes.NewBuffer([]byte{})
				recordWriter, err := recordWriterFactory.CreateWriter(dest.GetStageContext(), recordBuffer)

				for _, record := range records {
					err = recordWriter.WriteRecord(record)
					if err != nil {
						dest.GetStageContext().ReportError(err)
					}
				}

				flushAndCloseWriter(recordWriter)

				dest.keyCounter++
				kafkaProducer.Input() <- &sarama.ProducerMessage{
					Key:   sarama.StringEncoder(dest.Conf.Topic + strconv.Itoa(dest.keyCounter)),
					Topic: *topicName,
					Value: sarama.ByteEncoder(recordBuffer.Bytes()),
				}
			}

		} else {
			for _, record := range batch.GetRecords() {
				recordContext := context.WithValue(context.Background(), el.RecordContextVar, record)
				if topic, err := resolveTopic(dest.GetStageContext(), recordContext, &dest.Conf); err != nil {
					dest.GetStageContext().ToError(err, record)
					log.WithError(err).Error("resolve topic error")
				} else {
					recordBuffer := bytes.NewBuffer([]byte{})
					recordWriter, err := recordWriterFactory.CreateWriter(dest.GetStageContext(), recordBuffer)

					err = recordWriter.WriteRecord(record)
					if err != nil {
						dest.GetStageContext().ReportError(err)
					}

					flushAndCloseWriter(recordWriter)

					dest.keyCounter++
					kafkaProducer.Input() <- &sarama.ProducerMessage{
						Key:      sarama.StringEncoder(dest.Conf.Topic + strconv.Itoa(dest.keyCounter)),
						Topic:    *topic,
						Value:    sarama.ByteEncoder(recordBuffer.Bytes()),
						Metadata: record,
					}
				}
			}
		}
	}
	return nil
}

func (dest *KafkaDestination) Destroy() error {
	if dest.kafkaClient != nil && !dest.kafkaClient.Closed() {
		if err := dest.kafkaClient.Close(); err != nil {
			log.WithError(err).Error("Failed to close Kafka Client")
			return err
		}
	}
	return nil
}

func (dest *KafkaDestination) mapJVMConfigsToSaramaConfig() error {
	config := sarama.NewConfig()
	config.ClientID = ClientId
	config.Version = dest.kafkaVersion

	var trustStoreFilePath string
	insecureSkipVerify := false

	for name, value := range dest.Conf.KafkaProducerConfigs {
		switch name {
		// NET Config
		case SocketTimeoutMS:
			if i, err := strconv.Atoi(value); err == nil {
				config.Net.DialTimeout = time.Duration(i) * time.Millisecond
				config.Net.ReadTimeout = time.Duration(i) * time.Millisecond
				config.Net.WriteTimeout = time.Duration(i) * time.Millisecond
			}
		case SslEndpointIdentificationAlgorithm:
			if value == HTTPS {
				config.Net.TLS.Enable = true
			}
		case SecurityProtocol:
			if value == SecurityProtocolSASLPlainText || value == SecurityProtocolSASLSSL {
				config.Net.SASL.Enable = true
			} else if value == SecurityProtocolSSL {
				config.Net.TLS.Enable = true
			}

		case SASLJaasConfig:
			re := regexp.MustCompile(SSSLJaasConfigRegex)
			match := re.FindStringSubmatch(value)
			if len(match) > 2 {
				config.Net.SASL.User = match[1]
				config.Net.SASL.Password = match[2]
			}

		case SslTruststoreLocation:
			if !strings.HasSuffix(value, ".pem") {
				return errors.New("Data Collector edge requires truststore certificate in PEM format")
			}
			trustStoreFilePath = value

		case InsecureSkipVerify:
			if value == "true" {
				insecureSkipVerify = true
			}

		// Producer Config
		case MessageMaxBytes:
			if i, err := strconv.Atoi(value); err == nil {
				config.Producer.MaxMessageBytes = i
			}
		case RequestRequiredACKs:
			if i, err := strconv.Atoi(value); err == nil {
				switch i {
				case 0:
					config.Producer.RequiredAcks = sarama.NoResponse
				case 1:
					config.Producer.RequiredAcks = sarama.WaitForLocal
				case -1:
					config.Producer.RequiredAcks = sarama.WaitForAll
				}
			}
		case RequestTimeoutMS:
			if i, err := strconv.Atoi(value); err == nil {
				config.Producer.Timeout = time.Duration(i) * time.Millisecond
			}
		case CompressionType:
			switch value {
			case CompressionTypeNone:
				config.Producer.Compression = sarama.CompressionNone
			case CompressionTypeGzip:
				config.Producer.Compression = sarama.CompressionGZIP
			case CompressionTypeSnappy:
				config.Producer.Compression = sarama.CompressionSnappy
			case CompressionTypeLz4:
				config.Producer.Compression = sarama.CompressionLZ4
			}
		case QueueBufferingMaxMS:
			if i, err := strconv.Atoi(value); err == nil {
				config.Producer.Flush.Frequency = time.Duration(i) * time.Millisecond
			}
		case QueueBufferingMaxMessages:
			if i, err := strconv.Atoi(value); err == nil {
				config.Producer.Flush.MaxMessages = i
			}
		case MessageSendMaxRetries:
			if i, err := strconv.Atoi(value); err == nil {
				config.Producer.Retry.Max = i
			}
		case RetryBackoffMS:
			if i, err := strconv.Atoi(value); err == nil {
				config.Producer.Retry.Backoff = time.Duration(i) * time.Millisecond
			}
		}
	}

	if config.Net.TLS.Enable {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: insecureSkipVerify,
		}

		if len(trustStoreFilePath) > 0 {
			var caCertPool *x509.CertPool // nil CertPool will use system CA certs
			caCert, err := ioutil.ReadFile(trustStoreFilePath)
			if err != nil {
				return err
			}

			// appending to the system cert pool rather than replacing it
			caCertPool, err = x509.SystemCertPool()
			if err != nil {
				return err
			}
			caCertPool.AppendCertsFromPEM(caCert)

			tlsConfig.RootCAs = caCertPool
		}

		config.Net.TLS.Config = tlsConfig
	}

	dest.kafkaClientConf = config
	return dest.kafkaClientConf.Validate()
}

func resolveTopic(
	stageContext api.StageContext,
	recordContext context.Context,
	config *KafkaTargetConfig,
) (*string, error) {
	if !config.RuntimeTopicResolution {
		return &config.Topic, nil
	}

	result, err := stageContext.Evaluate(config.TopicExpression, "topicExpression", recordContext)
	if err != nil {
		return nil, err
	}

	if result == nil || cast.ToString(result) == "" {
		return nil, fmt.Errorf(topicResolutionError, config.TopicExpression)
	}

	topic := cast.ToString(result)
	return &topic, nil
}

func flushAndCloseWriter(recordWriter dataformats.RecordWriter) {
	err := recordWriter.Flush()
	if err != nil {
		log.WithError(err).Error("Error flushing record writer")
	}

	err = recordWriter.Close()
	if err != nil {
		log.WithError(err).Error("Error closing record writer")
	}
}

// +build aws

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
package kinesis

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/dataformats"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/util"
	"github.com/streamsets/datacollector-edge/stages/lib/awscommon"
	"github.com/streamsets/datacollector-edge/stages/lib/datagenerator"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
)

const (
	Library   = "streamsets-datacollector-kinesis-lib"
	StageName = "com_streamsets_pipeline_stage_destination_kinesis_KinesisDTarget"

	KinesisMaxBatchSize = 500
)

type Destination struct {
	*common.BaseStage
	KinesisConfig ProducerConfigBean `ConfigDefBean:"kinesisConfig"`
	kinesisClient *kinesis.Kinesis
}

type ProducerConfigBean struct {
	Region                    string                                  `ConfigDef:"type=STRING,required=true"`
	Endpoint                  string                                  `ConfigDef:"type=STRING,required=true"`
	StreamName                string                                  `ConfigDef:"type=STRING,required=true"`
	PartitionStrategy         string                                  `ConfigDef:"type=STRING,required=true"`
	PartitionExpression       string                                  `ConfigDef:"type=STRING,required=true"`
	ProducerConfigs           map[string]string                       `ConfigDef:"type=MAP,required=true"`
	PreserveOrdering          bool                                    `ConfigDef:"type=BOOLEAN,required=true"`
	AwsConfig                 awscommon.AWSConfig                     `ConfigDefBean:"awsConfig"`
	DataFormat                string                                  `ConfigDef:"type=STRING,required=true"`
	DataGeneratorFormatConfig datagenerator.DataGeneratorFormatConfig `ConfigDefBean:"dataGeneratorFormatConfig"`
}

func init() {
	stagelibrary.SetCreator(Library, StageName, func() api.Stage {
		return &Destination{BaseStage: &common.BaseStage{}}
	})
}

func (dest *Destination) Init(stageContext api.StageContext) []validation.Issue {
	issues := dest.BaseStage.Init(stageContext)
	issues = dest.KinesisConfig.DataGeneratorFormatConfig.Init(dest.KinesisConfig.DataFormat, stageContext, issues)
	awsSession, err := awscommon.GetAWSSession(
		dest.KinesisConfig.AwsConfig,
		dest.KinesisConfig.Region,
		dest.KinesisConfig.Endpoint,
		nil,
	)
	if err != nil {
		issues = append(issues, stageContext.CreateConfigIssue(err.Error()))
		return issues
	}
	dest.kinesisClient = kinesis.New(awsSession)
	// TODO: SDCE-390 Kinesis destination - consume Kinesis Producer Configuration
	return issues
}

func (dest *Destination) Write(batch api.Batch) error {
	if dest.KinesisConfig.PreserveOrdering {
		// write to Kinesis record by record
		for _, record := range batch.GetRecords() {
			dest.WriteRecord(record)
		}
	} else {
		// write to Kinesis in batches
		records := batch.GetRecords()
		done := false
		for !done {
			var batchRecords []api.Record
			if len(records) > KinesisMaxBatchSize {
				batchRecords = records[:KinesisMaxBatchSize]
				records = records[KinesisMaxBatchSize:]
			} else {
				batchRecords = records
				done = true
			}

			if err := dest.WriteInBatches(batchRecords); err != nil {
				return err
			}
		}
	}
	return nil
}

func (dest *Destination) WriteInBatches(records []api.Record) error {
	logrus.Info("In WriteInBatches")

	recordWriterFactory := dest.KinesisConfig.DataGeneratorFormatConfig.RecordWriterFactory

	entries := make([]*kinesis.PutRecordsRequestEntry, len(records))

	for i, record := range records {
		recordBuffer := bytes.NewBuffer([]byte{})
		recordWriter, err := recordWriterFactory.CreateWriter(dest.GetStageContext(), recordBuffer)
		if err != nil {
			logrus.WithError(err).Error("Error creating writer")
			dest.GetStageContext().ToError(err, record)
			break
		}
		err = recordWriter.WriteRecord(record)
		if err != nil {
			logrus.WithError(err).Error("Error writing record")
			dest.GetStageContext().ToError(err, record)
			break
		}
		flushAndCloseWriter(recordWriter)

		entries[i] = &kinesis.PutRecordsRequestEntry{
			Data:         recordBuffer.Bytes(),
			PartitionKey: aws.String(util.RandString(16)),
		}
	}

	putsOutput, err := dest.kinesisClient.PutRecords(&kinesis.PutRecordsInput{
		Records:    entries,
		StreamName: aws.String(dest.KinesisConfig.StreamName),
	})

	if err != nil {
		logrus.WithError(err).Error("error while writing records to Kinesis")
		for _, record := range records {
			dest.GetStageContext().ToError(err, record)
		}
		return nil
	}

	if putsOutput != nil && putsOutput.FailedRecordCount != nil {
		for i, entry := range putsOutput.Records {
			if entry.ErrorCode != nil {
				dest.GetStageContext().ToError(
					fmt.Errorf("Failed to write with code [%s]: %s", *entry.ErrorCode, *entry.ErrorMessage),
					records[i],
				)
			}
		}
	}

	return nil
}

func (dest *Destination) WriteRecord(record api.Record) {
	logrus.Info("In WriteRecord")

	recordWriterFactory := dest.KinesisConfig.DataGeneratorFormatConfig.RecordWriterFactory

	recordBuffer := bytes.NewBuffer([]byte{})
	recordWriter, err := recordWriterFactory.CreateWriter(dest.GetStageContext(), recordBuffer)
	if err != nil {
		logrus.WithError(err).Error("Error creating writer")
		dest.GetStageContext().ToError(err, record)
		return
	}
	err = recordWriter.WriteRecord(record)
	if err != nil {
		logrus.WithError(err).Error("Error writing record")
		dest.GetStageContext().ToError(err, record)
		return
	}
	flushAndCloseWriter(recordWriter)

	_, err = dest.kinesisClient.PutRecord(&kinesis.PutRecordInput{
		Data:         recordBuffer.Bytes(),
		PartitionKey: aws.String(util.RandString(16)),
		StreamName:   aws.String(dest.KinesisConfig.StreamName),
	})

	if err != nil {
		logrus.WithError(err).Error("error while writing record to Kinesis")
		dest.GetStageContext().ToError(err, record)
	}
}

func flushAndCloseWriter(recordWriter dataformats.RecordWriter) {
	err := recordWriter.Flush()
	if err != nil {
		logrus.WithError(err).Error("Error flushing record writer")
	}

	err = recordWriter.Close()
	if err != nil {
		logrus.WithError(err).Error("Error closing record writer")
	}
}

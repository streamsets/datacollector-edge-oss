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
package firehose

import (
	"bytes"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/dataformats"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/lib/awscommon"
	"github.com/streamsets/datacollector-edge/stages/lib/datagenerator"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
)

const (
	Library              = "streamsets-datacollector-kinesis-lib"
	StageName            = "com_streamsets_pipeline_stage_destination_kinesis_FirehoseDTarget"
	FirehoseMaxBatchSize = 500
)

type Destination struct {
	*common.BaseStage
	KinesisConfig  ConfigBean `ConfigDefBean:"kinesisConfig"`
	firehoseClient *firehose.Firehose
}

type ConfigBean struct {
	MaxRecordSize             float64                                 `ConfigDef:"type=NUMBER,required=true"`
	Region                    string                                  `ConfigDef:"type=STRING,required=true"`
	Endpoint                  string                                  `ConfigDef:"type=STRING,required=true"`
	StreamName                string                                  `ConfigDef:"type=STRING,required=true"`
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
	dest.firehoseClient = firehose.New(awsSession)
	return issues
}

func (dest *Destination) Write(batch api.Batch) error {
	// write to Kinesis in batches
	records := batch.GetRecords()
	done := false
	for !done {
		var batchRecords []api.Record
		if len(records) > FirehoseMaxBatchSize {
			batchRecords = records[:FirehoseMaxBatchSize]
			records = records[FirehoseMaxBatchSize:]
		} else {
			batchRecords = records
			done = true
		}

		if err := dest.WriteInBatches(batchRecords); err != nil {
			return err
		}
	}
	return nil
}

func (dest *Destination) WriteInBatches(records []api.Record) error {

	recordWriterFactory := dest.KinesisConfig.DataGeneratorFormatConfig.RecordWriterFactory

	entries := make([]*firehose.Record, len(records))

	for i, record := range records {
		recordBuffer := bytes.NewBuffer([]byte{})
		recordWriter, err := recordWriterFactory.CreateWriter(dest.GetStageContext(), recordBuffer)
		err = recordWriter.WriteRecord(record)
		if err != nil {
			logrus.WithError(err).Error("Error writing record")
			dest.GetStageContext().ToError(err, record)
			break
		}
		flushAndCloseWriter(recordWriter)

		entries[i] = &firehose.Record{
			Data: recordBuffer.Bytes(),
		}
	}

	putsOutput, err := dest.firehoseClient.PutRecordBatch(&firehose.PutRecordBatchInput{
		Records:            entries,
		DeliveryStreamName: aws.String(dest.KinesisConfig.StreamName),
	})

	if err != nil {
		logrus.WithError(err).Error("error while writing records to Kinesis")
		for _, record := range records {
			dest.GetStageContext().ToError(err, record)
		}
		return nil
	}

	if putsOutput != nil && putsOutput.FailedPutCount != nil {

	}

	return nil
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

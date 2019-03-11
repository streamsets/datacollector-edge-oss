// +build aws

// Copyright 2019 StreamSets Inc.
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
package s3

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cast"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/el"
	"github.com/streamsets/datacollector-edge/stages/lib/awscommon"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"strings"
)

const (
	Library                   = "streamsets-datacollector-aws-lib"
	StageName                 = "com_streamsets_pipeline_stage_destination_s3_AmazonS3DTarget"
	GzipExtension             = ".gz"
	bucketResolutionError     = "bucket name is empty for record %s"
	compressionWholeFileError = "Compression Option not supported for Whole file Data format"
	ConfGroupS3               = "S3"
	ConfCompress              = "s3TargetConfigBean.compress"
)

type Destination struct {
	*common.BaseStage
	S3TargetConfigBean TargetConfigBean `ConfigDefBean:"s3TargetConfigBean"`
	fileHelper         FileHelper
}

type TargetConfigBean struct {
	S3Config           ConnectionTargetConfig `ConfigDefBean:"s3Config"`
	SseConfig          TargetSSEConfigBean    `ConfigDefBean:"sseConfig"`
	ProxyConfig        awscommon.ProxyConfig  `ConfigDefBean:"proxyConfig"`
	TmConfig           TransferManagerConfig  `ConfigDefBean:"tmConfig"`
	PartitionTemplate  string                 `ConfigDef:"type=STRING,required=true,evaluation=EXPLICIT"`
	TimeZoneID         string                 `ConfigDef:"type=STRING,required=true"`
	TimeDriverTemplate string                 `ConfigDef:"type=STRING,required=true,evaluation=EXPLICIT"`
	FileNamePrefix     string                 `ConfigDef:"type=STRING,required=true"`
	FileNameSuffix     string                 `ConfigDef:"type=STRING,required=true"`
	Compress           bool                   `ConfigDef:"type=BOOLEAN,required=true"`
}

type ConnectionTargetConfig struct {
	AwsConfig      awscommon.AWSConfig `ConfigDefBean:"awsConfig"`
	Region         string              `ConfigDef:"type=STRING,required=true"`
	Endpoint       string              `ConfigDef:"type=STRING,required=true"`
	CommonPrefix   string              `ConfigDef:"type=STRING,required=true"`
	Delimiter      string              `ConfigDef:"type=STRING,required=true"`
	BucketTemplate string              `ConfigDef:"type=STRING,required=true,evaluation=EXPLICIT"`
}

type TargetSSEConfigBean struct {
	UseSSE            bool                    `ConfigDef:"type=BOOLEAN,required=true"`
	Encryption        string                  `ConfigDef:"type=STRING,required=true"`
	KmsKeyId          string                  `ConfigDef:"type=STRING,required=true"`
	EncryptionContext []EncryptionContextBean `ConfigDef:"type=MODEL" ListBeanModel:"name=encryptionContext"`
	CustomerKey       string                  `ConfigDef:"type=STRING,required=true"`
	CustomerKeyMd5    string                  `ConfigDef:"type=STRING,required=true"`
}

type TransferManagerConfig struct {
	ThreadPoolSize           float64 `ConfigDef:"type=NUMBER,required=true"`
	MultipartUploadThreshold float64 `ConfigDef:"type=NUMBER,required=true"`
	MinimumUploadPartSize    float64 `ConfigDef:"type=NUMBER,required=true"`
}

type EncryptionContextBean struct {
	Key   string `ConfigDef:"type=STRING,required=true"`
	Value string `ConfigDef:"type=STRING,required=true"`
}

type partition struct {
	bucket string
	path   string
}

func init() {
	stagelibrary.SetCreator(Library, StageName, func() api.Stage {
		return &Destination{BaseStage: &common.BaseStage{}}
	})
}

func (dest *Destination) Init(stageContext api.StageContext) []validation.Issue {
	issues := dest.BaseStage.Init(stageContext)
	awsSession, err := awscommon.GetAWSSession(
		dest.S3TargetConfigBean.S3Config.AwsConfig,
		dest.S3TargetConfigBean.S3Config.Region,
		dest.S3TargetConfigBean.S3Config.Endpoint,
		&dest.S3TargetConfigBean.ProxyConfig,
	)
	if err != nil {
		issues = append(issues, stageContext.CreateConfigIssue(err.Error()))
		return issues
	}

	uploader := s3manager.NewUploader(awsSession, func(u *s3manager.Uploader) {
		u.Concurrency = cast.ToInt(dest.S3TargetConfigBean.TmConfig.ThreadPoolSize)
		u.PartSize = cast.ToInt64(dest.S3TargetConfigBean.TmConfig.MinimumUploadPartSize)
		u.MaxUploadParts = cast.ToInt(dest.S3TargetConfigBean.TmConfig.MultipartUploadThreshold)
	})
	dataGeneratorService, _ := dest.GetDataGeneratorService()

	if dataGeneratorService.IsWholeFileFormat() {
		if dest.S3TargetConfigBean.Compress {
			issues = append(issues, stageContext.CreateConfigIssue(
				compressionWholeFileError,
				ConfGroupS3,
				ConfCompress,
			))
			return issues
		}
		s3Service := s3.New(awsSession)
		dest.fileHelper = &WholeFileHelper{
			stageContext:         dest.GetStageContext(),
			dataGeneratorService: dataGeneratorService,
			uploader:             uploader,
			s3Service:            s3Service,
			s3TargetConfigBean:   dest.S3TargetConfigBean,
		}
	} else {
		dest.fileHelper = &DefaultFileHelper{
			stageContext:         dest.GetStageContext(),
			dataGeneratorService: dataGeneratorService,
			uploader:             uploader,
			s3TargetConfigBean:   dest.S3TargetConfigBean,
		}
	}

	return issues
}

func (dest *Destination) Write(batch api.Batch) error {
	partitions := dest.partitionBatch(batch)
	for p, pRecords := range partitions {
		result, err := dest.fileHelper.Handle(pRecords, p.bucket, dest.getKeyPrefix(p.path))
		if err != nil {
			logrus.WithError(err).Error("failed to upload data to S3")
			for _, record := range pRecords {
				dest.GetStageContext().ToError(err, record)
			}
		} else if result != nil {
			logrus.Debug(result)
		}
	}
	return nil
}

func (dest *Destination) partitionBatch(batch api.Batch) map[partition][]api.Record {
	partitions := make(map[partition][]api.Record)
	for _, record := range batch.GetRecords() {
		recordContext := context.WithValue(context.Background(), el.RecordContextVar, record)
		bucketName, err := dest.resolveBucket(dest.GetStageContext(), recordContext, record)
		if err != nil {
			dest.GetStageContext().ToError(err, record)
			continue
		}

		pathName, err := dest.resolvePartition(dest.GetStageContext(), recordContext, record)
		if err != nil {
			dest.GetStageContext().ToError(err, record)
			continue
		}

		p := partition{bucketName, pathName}
		pRecords, ok := partitions[p]
		if !ok {
			pRecords = make([]api.Record, 0)
		}

		partitions[p] = append(pRecords, record)
	}
	return partitions
}

func (dest *Destination) resolveBucket(
	stageContext api.StageContext,
	recordContext context.Context,
	record api.Record,
) (string, error) {
	result, err := stageContext.Evaluate(
		dest.S3TargetConfigBean.S3Config.BucketTemplate,
		"bucketTemplate",
		recordContext,
	)
	if err != nil {
		return "", err
	}

	if result == nil || cast.ToString(result) == "" {
		return "", fmt.Errorf(bucketResolutionError, record.GetHeader().GetSourceId())
	}

	return cast.ToString(result), nil
}

func (dest *Destination) resolvePartition(
	stageContext api.StageContext,
	recordContext context.Context,
	record api.Record,
) (string, error) {
	result, err := stageContext.Evaluate(
		dest.S3TargetConfigBean.PartitionTemplate,
		"partitionTemplate",
		recordContext,
	)
	if err != nil {
		return "", err
	}
	return cast.ToString(result), nil
}

func (dest *Destination) getKeyPrefix(partition string) string {
	keyPrefix := dest.S3TargetConfigBean.S3Config.CommonPrefix

	if len(partition) > 0 {
		keyPrefix += partition
		if !strings.HasSuffix(partition, dest.S3TargetConfigBean.S3Config.Delimiter) {
			keyPrefix += dest.S3TargetConfigBean.S3Config.Delimiter
		}
	}

	if len(dest.S3TargetConfigBean.FileNamePrefix) > 0 {
		keyPrefix += dest.S3TargetConfigBean.FileNamePrefix + "-"
	}

	return keyPrefix
}

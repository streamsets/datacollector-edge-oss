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
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/streamsets/datacollector-edge/api"
	"io"
)

const (
	SseS3       = "S3"
	SseKMS      = "KMS"
	SseCustomer = "CUSTOMER"
	AES256      = "AES256"
	KMS         = "aws:kms"
)

type FileHelper interface {
	Handle(records []api.Record, bucket string, keyPrefix string) (*s3manager.UploadOutput, error)
}

func getUploadInput(
	s3TargetConfigBean TargetConfigBean,
	bucket string,
	fileName string,
	body io.Reader,
) *s3manager.UploadInput {
	uploadInput := &s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fileName),
		Body:   body,
	}

	if s3TargetConfigBean.SseConfig.UseSSE {
		switch s3TargetConfigBean.SseConfig.Encryption {
		case SseS3:
			uploadInput.ServerSideEncryption = aws.String(AES256)
		case SseKMS:
			uploadInput.ServerSideEncryption = aws.String(KMS)
			uploadInput.SSEKMSKeyId = aws.String(s3TargetConfigBean.SseConfig.KmsKeyId)
		case SseCustomer:
			uploadInput.SSECustomerAlgorithm = aws.String(AES256)
			uploadInput.SSECustomerKey = aws.String(s3TargetConfigBean.SseConfig.CustomerKey)
			uploadInput.SSECustomerKeyMD5 = aws.String(s3TargetConfigBean.SseConfig.CustomerKeyMd5)
		}
	}

	return uploadInput
}

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
package awscommon

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"strings"
)

const OtherRegion = "OTHER"

type AWSConfig struct {
	AwsAccessKeyId     string `ConfigDef:"type=STRING,required=true"`
	AwsSecretAccessKey string `ConfigDef:"type=STRING,required=true"`
}

func GetAWSSession(awsConfig AWSConfig, region string, endPoint string) (*session.Session, error) {
	return session.NewSession(&aws.Config{
		Region: GetRegion(region, endPoint),
		Credentials: credentials.NewStaticCredentials(
			awsConfig.AwsAccessKeyId,
			awsConfig.AwsSecretAccessKey,
			"",
		),
	})
}

func GetRegion(region string, endPoint string) *string {
	if region == OtherRegion {
		return aws.String(endPoint)
	}
	return aws.String(strings.ToLower(strings.Replace(region, "_", "-", -1)))
}

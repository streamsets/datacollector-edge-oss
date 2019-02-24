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
	"encoding/base64"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/spf13/cast"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const OtherRegion = "OTHER"

type AWSConfig struct {
	AwsAccessKeyId     string `ConfigDef:"type=STRING,required=true"`
	AwsSecretAccessKey string `ConfigDef:"type=STRING,required=true"`
}

type ProxyConfig struct {
	ConnectionTimeout float64 `ConfigDef:"type=NUMBER,required=true"`
	SocketTimeout     float64 `ConfigDef:"type=NUMBER,required=true"`
	RetryCount        float64 `ConfigDef:"type=NUMBER,required=true"`
	UseProxy          bool    `ConfigDef:"type=BOOLEAN,required=true"`
	ProxyHost         string  `ConfigDef:"type=STRING,required=true"`
	ProxyPort         float64 `ConfigDef:"type=NUMBER,required=true"`
	ProxyUser         string  `ConfigDef:"type=STRING,required=true"`
	ProxyPassword     string  `ConfigDef:"type=STRING,required=true"`
}

func GetAWSSession(
	awsConfig AWSConfig,
	region string,
	endPoint string,
	proxyConfig *ProxyConfig,
) (*session.Session, error) {
	config := &aws.Config{
		Region: GetRegion(region, endPoint),
		Credentials: credentials.NewStaticCredentials(
			awsConfig.AwsAccessKeyId,
			awsConfig.AwsSecretAccessKey,
			"",
		),
	}

	if proxyConfig != nil {
		transport := &http.Transport{}
		config.HTTPClient = &http.Client{Transport: transport}

		if proxyConfig.ConnectionTimeout > 0 {
			config.HTTPClient.Timeout = time.Duration(proxyConfig.ConnectionTimeout) * time.Second
		}

		config.MaxRetries = aws.Int(cast.ToInt(proxyConfig.RetryCount))

		if proxyConfig.UseProxy {
			proxyUrl, _ := url.Parse(fmt.Sprintf("http://%s:%f", proxyConfig.ProxyHost, proxyConfig.ProxyPort))
			transport.Proxy = http.ProxyURL(proxyUrl)
			if len(proxyConfig.ProxyUser) > 0 && len(proxyConfig.ProxyPassword) > 0 {
				auth := fmt.Sprintf("%s:%s", proxyConfig.ProxyUser, proxyConfig.ProxyPassword)
				basicAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
				transport.ProxyConnectHeader = http.Header{}
				transport.ProxyConnectHeader.Add("Proxy-Authorization", basicAuth)
			}
		}
	}

	return session.NewSession(config)
}

func GetRegion(region string, endPoint string) *string {
	if region == OtherRegion {
		return aws.String(endPoint)
	}
	return aws.String(strings.ToLower(strings.Replace(region, "_", "-", -1)))
}

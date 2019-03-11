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
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/dataformats"
	"github.com/streamsets/datacollector-edge/container/recordio/wholefilerecord"
)

type WholeFileHelper struct {
	stageContext         api.StageContext
	dataGeneratorService dataformats.DataFormatGeneratorService
	uploader             *s3manager.Uploader
	s3Service            *s3.S3
	s3TargetConfigBean   TargetConfigBean
}

func (f *WholeFileHelper) Handle(
	records []api.Record,
	bucket string,
	keyPrefix string,
) (*s3manager.UploadOutput, error) {
	if len(records) > 0 {
		// Only one record per batch if whole file
		wholeFileRecord := records[0]

		fileName := keyPrefix
		if fileNameElVal, err := f.dataGeneratorService.GetWholeFileName(wholeFileRecord); err != nil {
			return nil, err
		} else {
			fileName += fileNameElVal
		}

		if err := f.checkForWholeFileExistence(bucket, fileName); err != nil {
			return nil, err
		}

		var fileRef api.FileRef
		if fileRefVal, err := wholeFileRecord.Get(wholefilerecord.FileRefFieldPathName); err != nil {
			return nil, err
		} else if fileRefVal.Value == nil {
			return nil, errors.New("whole file reference value is empty")
		} else {
			fileRef = fileRefVal.Value.(api.FileRef)
		}

		is, err := fileRef.CreateInputStream()
		if err != nil {
			return nil, err
		}
		defer fileRef.CloseInputStream(is)

		// TODO: create event
		// SDCE-458 Whole File Data Generator - generate event record after transferring file

		// We are bypassing the generator because S3 has a convenient notion of taking input stream as a parameter.
		return f.uploader.Upload(getUploadInput(f.s3TargetConfigBean, bucket, fileName, is))
	}
	return nil, nil
}

func (f *WholeFileHelper) checkForWholeFileExistence(bucket string, fileName string) error {
	if listOutput, err := f.s3Service.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(fileName),
	}); err == nil && len(listOutput.Contents) > 0 && f.dataGeneratorService.GetWholeFileExistsAction() == "TO_ERROR" {
		return fmt.Errorf("object Key %s already exists", fileName)
	}
	return nil
}

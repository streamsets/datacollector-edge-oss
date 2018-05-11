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
package dataformats

import (
	"github.com/streamsets/datacollector-edge/api"
	"io"
)

const (
	DataFormatParserServiceName = "com.streamsets.pipeline.api.service.dataformats.DataFormatParserService"
)

type DataFormatParserService interface {
	GetParser(messageId string, reader io.Reader) (RecordReader, error)
}

type RecordReader interface {
	ReadRecord() (api.Record, error)
	Close() error
}

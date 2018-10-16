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
package delimitedrecord

import (
	"github.com/streamsets/datacollector-edge/api"
	"strings"
)

type RecordCreator struct {
	CsvFileFormat      string
	CsvCustomDelimiter string
	CsvRecordType      string
}

func (r *RecordCreator) CreateRecord(
	context api.StageContext,
	lineText string,
	messageId string,
	headers []*api.Field,
) (api.Record, error) {
	sep := ","
	if r.CsvFileFormat == Custom && len(r.CsvCustomDelimiter) > 0 {
		sep = r.CsvCustomDelimiter
	}
	columns := strings.Split(lineText, sep)
	return createRecord(
		context,
		messageId,
		1,
		r.CsvRecordType,
		columns,
		headers,
	)
}

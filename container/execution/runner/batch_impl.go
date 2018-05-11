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
package runner

import (
	"github.com/streamsets/datacollector-edge/api"
)

type BatchImpl struct {
	instanceName string
	records      []api.Record
	sourceOffset *string
}

func (b *BatchImpl) GetSourceOffset() *string {
	return b.sourceOffset
}

func (b *BatchImpl) GetRecords() []api.Record {
	return b.records
}

func NewBatchImpl(instanceName string, records []api.Record, sourceOffset *string) *BatchImpl {
	return &BatchImpl{
		instanceName: instanceName,
		records:      records,
		sourceOffset: sourceOffset,
	}
}

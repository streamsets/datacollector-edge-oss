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
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/execution/store"
	"time"
)

type ProductionSourceOffsetTracker struct {
	pipelineId    string
	currentOffset common.SourceOffset
	newOffset     *string
	finished      bool
	lastBatchTime time.Time
}

var emptyOffset = ""

func (o *ProductionSourceOffsetTracker) IsFinished() bool {
	return o.finished
}

func (o *ProductionSourceOffsetTracker) SetOffset(newOffset *string) {
	o.newOffset = newOffset
}

func (o *ProductionSourceOffsetTracker) CommitOffset() error {
	o.currentOffset.Offset[common.PollSourceOffsetKey] = o.newOffset
	o.finished = o.currentOffset.Offset[common.PollSourceOffsetKey] == nil
	o.newOffset = &emptyOffset
	return store.SaveOffset(o.pipelineId, o.currentOffset)
}

func (o *ProductionSourceOffsetTracker) GetOffset() *string {
	return o.currentOffset.Offset[common.PollSourceOffsetKey]
}

func (o *ProductionSourceOffsetTracker) GetLastBatchTime() time.Time {
	return o.lastBatchTime
}

func NewProductionSourceOffsetTracker(pipelineId string) (*ProductionSourceOffsetTracker, error) {
	if sourceOffset, err := store.GetOffset(pipelineId); err == nil {
		return &ProductionSourceOffsetTracker{
			pipelineId:    pipelineId,
			currentOffset: sourceOffset,
		}, nil
	} else {
		return nil, err
	}
}

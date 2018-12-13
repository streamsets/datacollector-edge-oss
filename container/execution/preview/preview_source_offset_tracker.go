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
package preview

import (
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/execution"
	"time"
)

type PreviewSourceOffsetTracker struct {
	pipelineId    string
	currentOffset common.SourceOffset
	newOffset     *string
	finished      bool
	lastBatchTime time.Time
}

var emptyOffset = ""

func (o *PreviewSourceOffsetTracker) IsFinished() bool {
	return false
}

func (o *PreviewSourceOffsetTracker) SetOffset(newOffset *string) {
	o.newOffset = newOffset
}

func (o *PreviewSourceOffsetTracker) CommitOffset() error {
	o.currentOffset.Offset[common.PollSourceOffsetKey] = o.newOffset
	o.finished = o.currentOffset.Offset[common.PollSourceOffsetKey] == &emptyOffset
	o.newOffset = &emptyOffset
	return nil
}

func (o *PreviewSourceOffsetTracker) GetOffset() *string {
	return o.currentOffset.Offset[common.PollSourceOffsetKey]
}

func (o *PreviewSourceOffsetTracker) GetLastBatchTime() time.Time {
	return o.lastBatchTime
}

func NewPreviewSourceOffsetTracker(pipelineId string) execution.SourceOffsetTracker {
	return &PreviewSourceOffsetTracker{
		pipelineId:    pipelineId,
		currentOffset: common.GetDefaultOffset(),
	}
}

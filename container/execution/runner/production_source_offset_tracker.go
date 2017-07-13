package runner

import (
	"github.com/streamsets/sdc2go/container/common"
	"github.com/streamsets/sdc2go/container/execution/store"
	"time"
)

type ProductionSourceOffsetTracker struct {
	pipelineId    string
	currentOffset common.SourceOffset
	newOffset     string
	finished      bool
	lastBatchTime time.Time
}

func (o *ProductionSourceOffsetTracker) IsFinished() bool {
	return false
}

func (o *ProductionSourceOffsetTracker) SetOffset(newOffset string) {
	o.newOffset = newOffset
}

func (o *ProductionSourceOffsetTracker) CommitOffset() {
	o.currentOffset.Offset[common.POLL_SOURCE_OFFSET_KEY] = o.newOffset
	o.finished = o.currentOffset.Offset[common.POLL_SOURCE_OFFSET_KEY] == ""
	o.newOffset = ""
	store.SaveOffset(o.pipelineId, o.currentOffset)
}

func (o *ProductionSourceOffsetTracker) GetOffset() string {
	return o.currentOffset.Offset[common.POLL_SOURCE_OFFSET_KEY]
}

func (o *ProductionSourceOffsetTracker) GetLastBatchTime() time.Time {
	return o.lastBatchTime
}

func NewProductionSourceOffsetTracker(pipelineId string) *ProductionSourceOffsetTracker {
	sourceOffset, _ := store.GetOffset(pipelineId)
	return &ProductionSourceOffsetTracker{
		pipelineId:    pipelineId,
		currentOffset: sourceOffset,
	}
}

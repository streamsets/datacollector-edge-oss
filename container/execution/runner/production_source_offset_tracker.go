package runner

import (
	"github.com/streamsets/dataextractor/container/common"
	"github.com/streamsets/dataextractor/container/execution/store"
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
	o.currentOffset.Offset = o.newOffset
	o.finished = o.currentOffset.Offset == ""
	o.newOffset = ""
	store.SaveOffset(o.pipelineId, o.currentOffset)
}

func (o *ProductionSourceOffsetTracker) GetOffset() string {
	return o.currentOffset.Offset
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

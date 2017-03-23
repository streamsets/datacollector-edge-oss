package runner

import "time"

type ProductionSourceOffsetTracker struct {
	pipelineName  string
	offsets       map[string]string
	lastBatchTime time.Time
}

func (o *ProductionSourceOffsetTracker) IsFinished() bool {
	return false
}

func (o *ProductionSourceOffsetTracker) CommitOffset(entity string, newOffset string) {
	// save offset
}

func (o *ProductionSourceOffsetTracker) GetOffsets() map[string]string {
	return o.offsets
}

func (o *ProductionSourceOffsetTracker) GetLastBatchTime() time.Time {
	return o.lastBatchTime
}

func NewProductionSourceOffsetTracker(pipelineName string) *ProductionSourceOffsetTracker {
	return &ProductionSourceOffsetTracker{pipelineName: pipelineName}
}

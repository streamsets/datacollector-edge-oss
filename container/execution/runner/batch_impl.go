package runner

import (
	"github.com/streamsets/dataextractor/api"
)

type BatchImpl struct {
	instanceName string
	records      []api.Record
	sourceOffset string
}

func (b *BatchImpl) GetSourceOffset() string {
	return b.sourceOffset
}

func (b *BatchImpl) GetRecords() []api.Record {
	return b.records
}

func NewBatchImpl(instanceName string, records []api.Record, sourceOffset string) *BatchImpl {
	return &BatchImpl{
		instanceName: instanceName,
		records:      records,
		sourceOffset: sourceOffset,
	}
}

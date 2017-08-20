package api

import "github.com/rcrowley/go-metrics"

type StageContext interface {
	// If we plan to support ELs later, we should remove and provide in build support for this
	GetResolvedValue(configValue interface{}) interface{}
	CreateRecord(recordSourceId string, value interface{}) (Record, error)
	GetMetrics() metrics.Registry
	ToError(err error, record Record)
	ReportError(err error)
}

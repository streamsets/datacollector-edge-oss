package api

import (
	"context"
	"github.com/rcrowley/go-metrics"
)

type StageContext interface {
	// If we plan to support ELs later, we should remove and provide in build support for this
	GetResolvedValue(configValue interface{}) (interface{}, error)
	CreateRecord(recordSourceId string, value interface{}) (Record, error)
	GetMetrics() metrics.Registry
	ToError(err error, record Record)
	ReportError(err error)
	GetOutputLanes() []string
	Evaluate(value string, configName string, ctx context.Context) (interface{}, error)
}

package process

import (
	"github.com/rcrowley/go-metrics"
	"time"
)

type Manager struct {
	config                     Config
	procMetricsCaptureInterval int64
	processMetricsRegistry     metrics.Registry
}

func (pManager *Manager) GetProcessMetrics() metrics.Registry {
	if pManager.procMetricsCaptureInterval <= 0 {
		metrics.CaptureDebugGCStatsOnce(pManager.processMetricsRegistry)
		metrics.CaptureRuntimeMemStatsOnce(pManager.processMetricsRegistry)
	}
	return pManager.processMetricsRegistry
}

func NewManager(config Config) (*Manager, error) {
	mgr := &Manager{
		config:                 config,
		processMetricsRegistry: metrics.NewRegistry(),
	}
	metrics.RegisterRuntimeMemStats(mgr.processMetricsRegistry)
	metrics.RegisterDebugGCStats(mgr.processMetricsRegistry)
	if config.ProcessMetricsCaptureInterval > 0 {
		metrics.CaptureRuntimeMemStats(
			mgr.processMetricsRegistry,
			time.Duration(config.ProcessMetricsCaptureInterval)*time.Millisecond)
		metrics.CaptureDebugGCStats(
			mgr.processMetricsRegistry,
			time.Duration(config.ProcessMetricsCaptureInterval)*time.Millisecond)
	}
	return mgr, nil
}

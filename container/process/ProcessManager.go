package process

import (
	"github.com/rcrowley/go-metrics"
	"strconv"
	"time"
)

type Manager struct {
	config Config
	procMetricsCaptureInterval int64
	processMetricsRegistry metrics.Registry
}

func (pManager *Manager) GetProcessMetrics() metrics.Registry {
	if pManager.procMetricsCaptureInterval <= 0 {
		metrics.CaptureDebugGCStatsOnce(pManager.processMetricsRegistry)
		metrics.CaptureRuntimeMemStatsOnce(pManager.processMetricsRegistry)
	}
	return pManager.processMetricsRegistry
}

func NewManager(config Config) (*Manager, error) {
	procMetricsCaptureInterval,err :=
		strconv.ParseInt(config.ProcessMetricsCaptureInterval, 10, 64)
	if err == nil {
		mgr := &Manager{
			config: config,
			processMetricsRegistry: metrics.NewRegistry(),
		}
		metrics.RegisterRuntimeMemStats(mgr.processMetricsRegistry)
		metrics.RegisterDebugGCStats(mgr.processMetricsRegistry)
		if procMetricsCaptureInterval > 0 {
			metrics.CaptureRuntimeMemStats(
				mgr.processMetricsRegistry,
				time.Duration(procMetricsCaptureInterval) * time.Millisecond)
			metrics.CaptureDebugGCStats(
				mgr.processMetricsRegistry,
				time.Duration(procMetricsCaptureInterval) * time.Millisecond)
		}
		return mgr, nil
	}
	return nil, err
}

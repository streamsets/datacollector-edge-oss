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

/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package util

import (
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
	"strings"
)

const (
	COUNTER_SUFFIX      = ".counter"
	METER_SUFFIX        = ".meter"
	HISTOGRAM_M5_SUFFIX = ".histogramM5"
	TIMER_SUFFIX        = ".timer"
	GAUGE_SUFFIX        = ".gauge"
)

func CreateCounter(registry metrics.Registry, name string) metrics.Counter {
	counter := metrics.NewCounter()
	registry.Register(metricName(name, COUNTER_SUFFIX), counter)
	return counter
}

func CreateMeter(registry metrics.Registry, name string) metrics.Meter {
	meter := metrics.NewMeter()
	registry.Register(metricName(name, METER_SUFFIX), meter)
	return meter
}

func CreateHistogram5Min(registry metrics.Registry, name string) metrics.Histogram {
	histogram := metrics.NewHistogram(metrics.NewExpDecaySample(1028, 0.015))
	registry.Register(metricName(name, HISTOGRAM_M5_SUFFIX), histogram)
	return histogram
}

func CreateTimer(registry metrics.Registry, name string) metrics.Timer {
	timer := metrics.NewTimer()
	registry.Register(metricName(name, TIMER_SUFFIX), timer)
	return timer
}

func metricName(name string, suffix string) string {
	if strings.HasSuffix(name, suffix) {
		return name
	}
	return name + suffix
}

type MetricsJson struct {
	Version    string                            `json:"version"`
	Gauges     map[string]map[string]interface{} `json:"gauges"`
	Counters   map[string]map[string]interface{} `json:"counters"`
	Histograms map[string]map[string]interface{} `json:"histograms"`
	Meters     map[string]map[string]interface{} `json:"meters"`
	Timers     map[string]map[string]interface{} `json:"timers"`
}

func FormatMetricsRegistry(r metrics.Registry) MetricsJson {
	gauges := make(map[string]map[string]interface{})
	counters := make(map[string]map[string]interface{})
	histograms := make(map[string]map[string]interface{})
	meters := make(map[string]map[string]interface{})
	timers := make(map[string]map[string]interface{})

	r.Each(func(name string, i interface{}) {
		log.Debug("Metrics:" + name)
		values := make(map[string]interface{})
		switch metric := i.(type) {
		case metrics.Counter:
			values["count"] = metric.Count()
			counters[name] = values
		case metrics.Gauge:
			values["value"] = metric.Value()
			gauges[name] = values
		case metrics.GaugeFloat64:
			values["value"] = metric.Value()
			counters[name] = values
		case metrics.Healthcheck:
			values["error"] = nil
			metric.Check()
			if err := metric.Error(); nil != err {
				values["error"] = metric.Error().Error()
			}
		case metrics.Histogram:
			h := metric.Snapshot()
			ps := h.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
			values["count"] = h.Count()
			values["min"] = h.Min()
			values["max"] = h.Max()
			values["mean"] = h.Mean()
			values["stddev"] = h.StdDev()
			values["p50"] = ps[0]
			values["p75"] = ps[1]
			values["p98"] = ps[2]
			values["p99"] = ps[3]
			values["p999"] = ps[4]
			histograms[name] = values
		case metrics.Meter:
			m := metric.Snapshot()
			values["count"] = m.Count()
			values["m1_rate"] = m.Rate1()
			values["m5_rate"] = m.Rate5()
			values["m15_rate"] = m.Rate15()
			values["mean_rate"] = m.RateMean()
			values["units"] = "events/second"
			meters[name] = values
		case metrics.Timer:
			t := metric.Snapshot()
			ps := t.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
			values["count"] = t.Count()
			values["min"] = t.Min()
			values["max"] = t.Max()
			values["mean"] = t.Mean()
			values["stddev"] = t.StdDev()
			values["p50"] = ps[0]
			values["p75"] = ps[1]
			values["p98"] = ps[2]
			values["p99"] = ps[3]
			values["p999"] = ps[4]
			values["m1_rate"] = t.Rate1()
			values["m5_rate"] = t.Rate5()
			values["m15_rate"] = t.Rate15()
			values["mean_rate"] = t.RateMean()
			values["duration_units"] = "seconds"
			values["rate_units"] = "calls/second"
			timers[name] = values
		}
	})

	return MetricsJson{
		Version:    "3.0.0",
		Gauges:     gauges,
		Counters:   counters,
		Histograms: histograms,
		Meters:     meters,
		Timers:     timers,
	}
}

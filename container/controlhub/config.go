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
package controlhub

const (
	DefaultBaseUrl              = "http://localhost:18631"
	AllLabel                    = "all"
	JobRunnerApp                = "jobrunner-app"
	TimeSeriesApp               = "timeseries-app"
	DefaultPingFrequency        = 5000
	DefaultStatusEventsInterval = 60000
)

type Config struct {
	Enabled                bool     `toml:"enabled"`
	BaseUrl                string   `toml:"base-url"`
	AppAuthToken           string   `toml:"app-auth-token"`
	JobLabels              []string `toml:"job-labels"`
	EventsRecipient        string   `toml:"events-recipient"`
	ProcessEventsRecipient []string `toml:"process-events-recipients"`
	PingFrequency          int      `toml:"ping-frequency"`
	StatusEventsInterval   int      `toml:"status-events-interval"`
}

// NewConfig returns a new Config with default settings.
func NewConfig() Config {
	return Config{
		Enabled:                false,
		BaseUrl:                DefaultBaseUrl,
		AppAuthToken:           "",
		JobLabels:              []string{AllLabel},
		EventsRecipient:        JobRunnerApp,
		ProcessEventsRecipient: []string{JobRunnerApp, TimeSeriesApp},
		PingFrequency:          DefaultPingFrequency,
		StatusEventsInterval:   DefaultStatusEventsInterval,
	}
}

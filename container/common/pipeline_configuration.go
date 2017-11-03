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
package common

const (
	PIPELINE_CONFIG_SCHEMA_VERSION = 3
	PIPELINE_CONFIG_VERSION        = 6
)

type PipelineConfiguration struct {
	SchemaVersion        int                    `json:"schemaVersion"`
	Version              int                    `json:"version"`
	PipelineId           string                 `json:"pipelineId"`
	Title                string                 `json:"title"`
	Description          string                 `json:"description"`
	UUID                 string                 `json:"uuid"`
	Configuration        []Config               `json:"configuration"`
	UiInfo               map[string]interface{} `json:"uiInfo"`
	Stages               []StageConfiguration   `json:"stages"`
	ErrorStage           StageConfiguration     `json:"errorStage"`
	StatsAggregatorStage StageConfiguration     `json:"statsAggregatorStage"`
	Previewable          bool                   `json:"previewable"`
	Info                 PipelineInfo           `json:"info"`
	Metadata             map[string]interface{} `json:"metadata"`
}

type PipelineInfo struct {
	PipelineId   string                 `json:"pipelineId"`
	Title        string                 `json:"title"`
	Description  string                 `json:"description"`
	Created      int64                  `json:"created"`
	LastModified int64                  `json:"lastModified"`
	Creator      string                 `json:"creator"`
	LastModifier string                 `json:"lastModifier"`
	LastRev      string                 `json:"lastRev"`
	UUID         string                 `json:"uuid"`
	Valid        bool                   `json:"valid"`
	Metadata     map[string]interface{} `json:"metadata"`
}

type Config struct {
	Name  string      `json:"name"`
	Value interface{} `json:"value"`
}

type ServiceConfiguration struct {
	Service        string   `json:"service"`
	ServiceVersion string   `json:"serviceVersion"`
	Configuration  []Config `json:"configuration"`
}

type StageConfiguration struct {
	InstanceName  string                 `json:"instanceName"`
	Library       string                 `json:"library"`
	StageName     string                 `json:"stageName"`
	StageVersion  string                 `json:"stageVersion"`
	Configuration []Config               `json:"configuration"`
	Services      []ServiceConfiguration `json:"services"`
	UiInfo        map[string]interface{} `json:"uiInfo"`
	InputLanes    []string               `json:"inputLanes"`
	OutputLanes   []string               `json:"outputLanes"`
	EventLanes    []string               `json:"eventLanes"`
}

func (s StageConfiguration) GetConfigurationMap() map[string]Config {
	configurationMap := make(map[string]Config)
	for _, config := range s.Configuration {
		configurationMap[config.Name] = config
	}
	return configurationMap
}

type PipelineEnvelope struct {
	PipelineConfig     PipelineConfiguration  `json:"pipelineConfig"`
	PipelineRules      map[string]interface{} `json:"pipelineRules"`
	LibraryDefinitions map[string]interface{} `json:"libraryDefinitions"`
}

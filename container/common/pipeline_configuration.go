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
	PipelineConfigSchemaVersion = 5
	PipelineConfigVersion       = 9
	ErrorRecordPolicyOriginal   = "ORIGINAL_RECORD"
	ErrorRecordPolicyStage      = "STAGE_RECORD"
	FragmentSourceStageName     = "com_streamsets_pipeline_stage_origin_fragment_FragmentSource"
	FragmentProcessorStageName  = "com_streamsets_pipeline_stage_processor_fragment_FragmentProcessor"
	FragmentTargetStageName     = "com_streamsets_pipeline_stage_destination_fragment_FragmentTarget"
	ConfFragmentId              = "conf.fragmentId"
	ConfFragmentInstanceId      = "conf.fragmentInstanceId"
)

type PipelineConfiguration struct {
	SchemaVersion        int                              `json:"schemaVersion"`
	Version              int                              `json:"version"`
	PipelineId           string                           `json:"pipelineId"`
	Title                string                           `json:"title"`
	Description          string                           `json:"description"`
	UUID                 string                           `json:"uuid"`
	Configuration        []Config                         `json:"configuration"`
	UiInfo               map[string]interface{}           `json:"uiInfo"`
	Stages               []*StageConfiguration            `json:"stages"`
	ErrorStage           *StageConfiguration              `json:"errorStage"`
	StatsAggregatorStage *StageConfiguration              `json:"statsAggregatorStage"`
	Previewable          bool                             `json:"previewable"`
	Info                 PipelineInfo                     `json:"info"`
	Metadata             map[string]interface{}           `json:"metadata"`
	Fragments            []*PipelineFragmentConfiguration `json:"fragments"`
}


type PipelineFragmentConfiguration struct {
	SchemaVersion      int                              `json:"schemaVersion"`
	Version            int                              `json:"version"`
	PipelineId         string                           `json:"fragmentId"`
	FragmentInstanceId string                           `json:"fragmentInstanceId"`
	Title              string                           `json:"title"`
	Description        string                           `json:"description"`
	UUID               string                           `json:"uuid"`
	Configuration      []Config                         `json:"configuration"`
	UiInfo             map[string]interface{}           `json:"uiInfo"`
	Stages             []*StageConfiguration            `json:"stages"`
	Previewable        bool                             `json:"previewable"`
	Info               PipelineInfo                     `json:"info"`
	Metadata           map[string]interface{}           `json:"metadata"`
	Fragments          []*PipelineFragmentConfiguration `json:"fragments"`
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


func (p *PipelineConfiguration) ProcessFragmentStages() {
	if p.Fragments != nil && len(p.Fragments) > 0 {
		resolvedStages := make([]*StageConfiguration, 0)
		for _, stageInstance := range p.Stages {
			if isFragmentGroupStage(stageInstance) {
				stageConfig := stageInstance.GetConfigurationMap()
				fragmentIdConfig := stageConfig[ConfFragmentId]
				fragmentInstanceIdConfig := stageConfig[ConfFragmentInstanceId]

				if fragmentIdConfig.Value != nil && fragmentInstanceIdConfig.Value != nil {
					fragmentId := fragmentIdConfig.Value.(string)
					fragmentInstanceId := fragmentInstanceIdConfig.Value.(string)
					for _, fragment := range p.Fragments {
						if fragment.PipelineId == fragmentId && fragment.FragmentInstanceId == fragmentInstanceId {
							resolvedStages = append(resolvedStages, fragment.Stages...)
						}
					}
				}

			} else {
				resolvedStages = append(resolvedStages, stageInstance)
			}
		}

		// TODO: sort stage instances

		p.Stages = resolvedStages
	}
}

func isFragmentGroupStage(stageInstance *StageConfiguration) bool {
	return stageInstance.StageName == FragmentSourceStageName ||
		stageInstance.StageName == FragmentProcessorStageName ||
		stageInstance.StageName == FragmentTargetStageName
}


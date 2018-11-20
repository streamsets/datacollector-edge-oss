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
package common

const (
	PipelineConfigSchemaVersion = 6
	PipelineConfigVersion       = 10
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
	TestOriginStage      *StageConfiguration              `json:"testOriginStage"`
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
	TestOriginStage    *StageConfiguration              `json:"testOriginStage"`
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
	Name         string                 `json:"name"`
	SdcVersion   string                 `json:"sdcVersion"`
	SdcId        string                 `json:"sdcId"`
}

type Config struct {
	Name  string      `json:"name"`
	Value interface{} `json:"value"`
}

type ServiceConfiguration struct {
	Service        string      `json:"service"`
	ServiceVersion interface{} `json:"serviceVersion"` // interface{} is for backward compatible - SDC-10526
	Configuration  []Config    `json:"configuration"`
}

func (s ServiceConfiguration) GetConfigurationMap() map[string]Config {
	configurationMap := make(map[string]Config)
	for _, config := range s.Configuration {
		configurationMap[config.Name] = config
	}
	return configurationMap
}

type StageConfiguration struct {
	InstanceName  string                  `json:"instanceName"`
	Library       string                  `json:"library"`
	StageName     string                  `json:"stageName"`
	StageVersion  string                  `json:"stageVersion"`
	Configuration []Config                `json:"configuration"`
	Services      []*ServiceConfiguration `json:"services"`
	UiInfo        map[string]interface{}  `json:"uiInfo"`
	InputLanes    []string                `json:"inputLanes"`
	OutputLanes   []string                `json:"outputLanes"`
	EventLanes    []string                `json:"eventLanes"`
}

func (s StageConfiguration) GetConfigurationMap() map[string]Config {
	configurationMap := make(map[string]Config)
	for _, config := range s.Configuration {
		configurationMap[config.Name] = config
	}
	return configurationMap
}

func (s StageConfiguration) GetOutputAndEventLanes() []string {
	outputAndEventLanes := make([]string, len(s.OutputLanes)+len(s.EventLanes))
	outputAndEventLanes = append(outputAndEventLanes, s.OutputLanes...)
	outputAndEventLanes = append(outputAndEventLanes, s.EventLanes...)
	return outputAndEventLanes
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
		p.Stages = sortStageInstances(resolvedStages)
	}
}

func isFragmentGroupStage(stageInstance *StageConfiguration) bool {
	return stageInstance.StageName == FragmentSourceStageName ||
		stageInstance.StageName == FragmentProcessorStageName ||
		stageInstance.StageName == FragmentTargetStageName
}

func sortStageInstances(stageInstances []*StageConfiguration) []*StageConfiguration {
	sorted := make([]*StageConfiguration, 0)
	removedMap := make(map[string]bool)
	producedOutputs := make([]string, 0)
	ok := true
	iteration := 0
	for ok {
		prior := len(sorted)
		for _, stageInstance := range stageInstances {
			if !removedMap[stageInstance.InstanceName] {
				alreadyProduced := make([]string, 0)
				for _, p := range producedOutputs {
					for _, inputLane := range stageInstance.InputLanes {
						if inputLane == p {
							alreadyProduced = append(alreadyProduced, p)
						}
					}
				}
				if len(alreadyProduced) == len(stageInstance.InputLanes) {
					producedOutputs = append(producedOutputs, stageInstance.OutputLanes...)
					producedOutputs = append(producedOutputs, stageInstance.EventLanes...)
					removedMap[stageInstance.InstanceName] = true
					sorted = append(sorted, stageInstance)
				}
			}
		}
		iteration++
		if prior == len(sorted) && iteration >= len(sorted) {
			ok = false
			for _, stageInstance := range stageInstances {
				if !removedMap[stageInstance.InstanceName] {
					sorted = append(sorted, stageInstance)
				}
			}
		}
	}
	return sorted
}

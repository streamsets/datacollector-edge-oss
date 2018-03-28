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
package creation

import (
	"github.com/streamsets/datacollector-edge/container/common"
)

type PipelineBean struct {
	Config               PipelineConfigBean
	Stages               []StageBean
	ErrorStage           StageBean
	StatsAggregatorStage StageBean
}

func NewPipelineBean(
	pipelineConfig common.PipelineConfiguration,
	runtimeParameters map[string]interface{},
) (PipelineBean, error) {
	var pipelineBean PipelineBean
	var err error

	pipelineBean.Config = NewPipelineConfigBean(pipelineConfig)

	stageBeans := make([]StageBean, len(pipelineConfig.Stages))
	for i, stageConfig := range pipelineConfig.Stages {
		stageBeans[i], err = NewStageBean(stageConfig, runtimeParameters)
		if err != nil {
			return pipelineBean, err
		}
	}
	pipelineBean.Stages = stageBeans

	if pipelineConfig.ErrorStage.InstanceName != "" {
		pipelineBean.ErrorStage, err = NewStageBean(pipelineConfig.ErrorStage, runtimeParameters)
		if err != nil {
			return pipelineBean, err
		}
	}

	if pipelineConfig.StatsAggregatorStage != nil && pipelineConfig.StatsAggregatorStage.InstanceName != "" {
		pipelineBean.StatsAggregatorStage, err = NewStageBean(pipelineConfig.StatsAggregatorStage, runtimeParameters)
		if err != nil {
			return pipelineBean, err
		}
	}

	return pipelineBean, err
}

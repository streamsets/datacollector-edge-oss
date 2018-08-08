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
package creation

import (
	"context"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/el"
)

type PipelineBean struct {
	Config               PipelineConfigBean
	Stages               []StageBean
	ErrorStage           StageBean
	StatsAggregatorStage StageBean
	ElContext            context.Context
}

func NewPipelineBean(
	pipelineConfig common.PipelineConfiguration,
	runtimeParameters map[string]interface{},
) (PipelineBean, []validation.Issue) {
	issues := make([]validation.Issue, 0)
	var pipelineBean PipelineBean
	var err error

	pipelineBean.Config = NewPipelineConfigBean(pipelineConfig)

	elContext := context.WithValue(context.Background(), el.PipelineIdContextVar, pipelineConfig.PipelineId)
	elContext = context.WithValue(elContext, el.PipelineTitleContextVar, pipelineConfig.Title)
	elContext = context.WithValue(elContext, el.PipelineUserContextVar, pipelineConfig.Info.LastModifier)
	pipelineBean.ElContext = elContext

	stageBeans := make([]StageBean, len(pipelineConfig.Stages))
	for i, stageConfig := range pipelineConfig.Stages {
		stageBeans[i], err = NewStageBean(stageConfig, runtimeParameters, elContext)
		if err != nil {
			issues = append(issues, validation.Issue{
				InstanceName: stageConfig.InstanceName,
				Level:        common.StageConfig,
				Count:        1,
				Message:      err.Error(),
			})
			return pipelineBean, issues
		}
	}
	pipelineBean.Stages = stageBeans

	if pipelineConfig.ErrorStage.InstanceName != "" {
		pipelineBean.ErrorStage, err = NewStageBean(pipelineConfig.ErrorStage, runtimeParameters, elContext)
		if err != nil {
			issues = append(issues, validation.Issue{
				InstanceName: pipelineConfig.ErrorStage.InstanceName,
				Level:        common.StageConfig,
				Count:        1,
				Message:      err.Error(),
			})
			return pipelineBean, issues
		}
	}

	if pipelineConfig.StatsAggregatorStage != nil && pipelineConfig.StatsAggregatorStage.InstanceName != "" {
		pipelineBean.StatsAggregatorStage, err =
			NewStageBean(pipelineConfig.StatsAggregatorStage, runtimeParameters, elContext)
		if err != nil {
			issues = append(issues, validation.Issue{
				InstanceName: pipelineConfig.StatsAggregatorStage.InstanceName,
				Level:        common.StageConfig,
				Count:        1,
				Message:      err.Error(),
			})
			return pipelineBean, issues
		}
	}

	return pipelineBean, issues
}

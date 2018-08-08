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
package runner

import (
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/execution"
)

const (
	IssueErrorTemplate = "Initialization Error '%s' on Instance : '%s' "
)

type ProductionPipeline struct {
	PipelineConfig common.PipelineConfiguration
	Pipeline       *Pipeline
	MetricRegistry metrics.Registry
}

func (p *ProductionPipeline) Init() []validation.Issue {
	issues := p.Pipeline.Init()
	if len(issues) != 0 {
		for _, issue := range issues {
			log.Printf("[ERROR] "+IssueErrorTemplate, issue.Message, issue.InstanceName)
		}
	}
	return issues
}

func (p *ProductionPipeline) Run() {
	log.Debug("Production Pipeline Run")
	p.Pipeline.Run()
}

func (p *ProductionPipeline) Stop() {
	log.Debug("Production Pipeline Stop")
	p.Pipeline.Stop()
}

func NewProductionPipeline(
	pipelineId string,
	config execution.Config,
	runner execution.Runner,
	pipelineConfiguration common.PipelineConfiguration,
	runtimeParameters map[string]interface{},
) (*ProductionPipeline, []validation.Issue) {
	if sourceOffsetTracker, err := NewProductionSourceOffsetTracker(pipelineId); err == nil {
		metricRegistry := metrics.NewRegistry()
		pipeline, issues := NewPipeline(
			config,
			runner.GetPipelineConfig(),
			sourceOffsetTracker,
			runtimeParameters,
			metricRegistry,
		)
		return &ProductionPipeline{
			PipelineConfig: pipelineConfiguration,
			Pipeline:       pipeline,
			MetricRegistry: metricRegistry,
		}, issues
	} else {
		issues := make([]validation.Issue, 0)
		issues = append(issues, validation.Issue{
			Count:   1,
			Message: err.Error(),
		})
		return nil, issues
	}
}

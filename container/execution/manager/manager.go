package manager

import (
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
)

type Manager interface {
	GetRunner(pipelineId string) *runner.StandaloneRunner
	StartPipeline(
		pipelineId string,
		runtimeParameters map[string]interface{},
	) (*common.PipelineState, error)
	StopPipeline(pipelineId string) (*common.PipelineState, error)
	ResetOffset(pipelineId string) error
}

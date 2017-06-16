package manager

import (
	"github.com/streamsets/sdc2go/container/common"
	"github.com/streamsets/sdc2go/container/execution/runner"
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

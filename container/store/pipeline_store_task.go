package store

import "github.com/streamsets/sdc2go/container/common"

type PipelineStoreTask interface {
	GetPipelines() ([]common.PipelineInfo, error)
	GetInfo(pipelineId string) (common.PipelineInfo, error)
	Create(pipelineId string, pipelineTitle string, description string) (common.PipelineConfiguration, error)
	Save(pipelineId string, pipelineConfiguration common.PipelineConfiguration) (common.PipelineConfiguration, error)
	LoadPipelineConfig(pipelineId string) (common.PipelineConfiguration, error)
}

package creation

import (
	"encoding/json"
	"github.com/streamsets/dataextractor/container/common"
	"os"
)

const (
	PIPELINE_FILE = "pipeline.json"
)

func LoadPipelineConfig(pipelineId string) (common.PipelineConfiguration, error) {
	pipelineConfiguration := common.PipelineConfiguration{}
	file, err := os.Open(getPipelineFile(pipelineId))
	if err != nil {
		return pipelineConfiguration, err
	}

	decoder := json.NewDecoder(file)
	err1 := decoder.Decode(&pipelineConfiguration)
	if err1 != nil {
		return pipelineConfiguration, err1
	}

	return pipelineConfiguration, err1
}

func getPipelineFile(pipelineId string) string {
	// return getPipelineDir(pipelineId) + PIPELINE_FILE

	// TODO: Use data directory for pipelines
	return "etc/pipeline.json"
}

func getPipelineDir(pipelineId string) string {
	return "data/pipelines/" + pipelineId + "/"
}

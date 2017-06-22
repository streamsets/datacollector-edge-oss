package creation

import (
	"encoding/json"
	"github.com/streamsets/sdc2go/container/common"
	"os"
	"errors"
)

const (
	PIPELINE_FILE = "pipeline.json"
)

func LoadPipelineConfig(runtimeInfo common.RuntimeInfo, pipelineId string) (common.PipelineConfiguration, error) {
	pipelineConfiguration := common.PipelineConfiguration{}
	file, err := os.Open(getPipelineFile(runtimeInfo, pipelineId))
	if err != nil {
		return pipelineConfiguration, err
	}

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&pipelineConfiguration)
	if err != nil {
		return pipelineConfiguration, err
	}

	if pipelineConfiguration.PipelineId == "" {
		err = errors.New("Invalid pipeline configuration")
	}

	return pipelineConfiguration, err
}

func getPipelineFile(runtimeInfo common.RuntimeInfo, pipelineId string) string {
	return getPipelineDir(runtimeInfo, pipelineId) + PIPELINE_FILE
}

func getPipelineDir(runtimeInfo common.RuntimeInfo, pipelineId string) string {
	return runtimeInfo.BaseDir + "/data/pipelines/" + pipelineId + "/"
}

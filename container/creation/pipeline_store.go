package creation

import (
	"encoding/json"
	"github.com/streamsets/sdc2go/container/common"
	"os"
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
	err1 := decoder.Decode(&pipelineConfiguration)
	if err1 != nil {
		return pipelineConfiguration, err1
	}

	return pipelineConfiguration, err1
}

func getPipelineFile(runtimeInfo common.RuntimeInfo, pipelineId string) string {
	return getPipelineDir(runtimeInfo, pipelineId) + PIPELINE_FILE
}

func getPipelineDir(runtimeInfo common.RuntimeInfo, pipelineId string) string {
	return runtimeInfo.BaseDir + "/data/pipelines/" + pipelineId + "/"
}

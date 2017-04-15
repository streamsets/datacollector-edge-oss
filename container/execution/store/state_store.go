package store

import (
	"encoding/json"
	"github.com/streamsets/dataextractor/container/common"
	"io/ioutil"
	"os"
	"time"
)

const (
	DEFAULT_PIPELINE_ID = "dataextractor"
	PIPELINE_STATE_FILE = "pipelineState.json"
)

func GetState(pipelineId string) (*common.PipelineState, error) {
	if _, err := os.Stat(getPipelineOffsetFile(pipelineId)); os.IsNotExist(err) {
		pipelineState := &common.PipelineState{
			PipelineId: DEFAULT_PIPELINE_ID,
			Status:     common.EDITED,
			Message:    "",
			TimeStamp:  time.Now().UTC(),
		}
		err = os.MkdirAll(getRunInfoDir(pipelineId), os.ModePerm)
		if err == nil {
			err = SaveState(pipelineId, pipelineState)
		}
		return pipelineState, err
	} else {
		file, readError := ioutil.ReadFile(getPipelineOffsetFile(pipelineId))

		if readError != nil {
			return nil, readError
		}

		var pipelineState common.PipelineState
		json.Unmarshal(file, &pipelineState)
		return &pipelineState, nil
	}
}

func SaveState(pipelineId string, pipelineState *common.PipelineState) error {
	pipelineStateJson, err := json.Marshal(pipelineState)
	check(err)
	err = ioutil.WriteFile(getPipelineStateFile(pipelineId), pipelineStateJson, 0644)
	if err != nil {
		panic(err)
	}
	return err
}

func getPipelineStateFile(pipelineId string) string {
	return getRunInfoDir(pipelineId) + PIPELINE_STATE_FILE
}

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
	PIPELINE_STATE_FILE = "data/pipelineState.json"
)

func GetState() (*common.PipelineState, error) {
	if _, err := os.Stat(PIPELINE_STATE_FILE); os.IsNotExist(err) {
		pipelineState := &common.PipelineState{
			PipelineId: DEFAULT_PIPELINE_ID,
			Status:     common.EDITED,
			Message:    "",
			TimeStamp:  time.Now().UTC(),
		}
		err := SaveState(pipelineState)
		return pipelineState, err

	} else {
		file, readError := ioutil.ReadFile(PIPELINE_STATE_FILE)

		if readError != nil {
			return nil, readError
		}

		var pipelineState common.PipelineState
		json.Unmarshal(file, &pipelineState)
		return &pipelineState, nil
	}
}

func SaveState(pipelineState *common.PipelineState) error {
	pipelineStateJson, err := json.Marshal(pipelineState)
	check(err)
	err1 := ioutil.WriteFile(PIPELINE_STATE_FILE, pipelineStateJson, 0644)
	return err1
}

package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/streamsets/datacollector-edge/container/common"
	"io"
	"io/ioutil"
	"os"
	"time"
)

const (
	PIPELINE_STATE_FILE         = "pipelineState.json"
	PIPELINE_STATE_HISTORY_FILE = "pipelineStateHistory.json"
)

func checkFileExists(filePath string) (bool, error) {
	_, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	} else {
		return true, nil
	}
}

func GetState(pipelineId string) (*common.PipelineState, error) {
	fileExists, err := checkFileExists(getPipelineStateFile(pipelineId))
	if err != nil {
		return nil, err
	}
	if !fileExists {
		pipelineState := &common.PipelineState{
			PipelineId: pipelineId,
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
		file, readError := ioutil.ReadFile(getPipelineStateFile(pipelineId))

		if readError != nil {
			return nil, readError
		}

		var pipelineState common.PipelineState
		err := json.Unmarshal(file, &pipelineState)
		fmt.Println(pipelineState.PipelineId)
		return &pipelineState, err
	}
}

func SaveState(pipelineId string, pipelineState *common.PipelineState) error {
	pipelineStateJson, err := json.Marshal(pipelineState)
	check(err)
	err = ioutil.WriteFile(getPipelineStateFile(pipelineId), pipelineStateJson, 0644)
	if err != nil {
		panic(err)
	}

	//open for append or create and open for write if it does not exist
	openFlag := os.O_APPEND | os.O_CREATE | os.O_WRONLY

	//save in history file as well.
	historyFile, err := os.OpenFile(getPipelineStateHistoryFile(pipelineId), openFlag, 0666)
	defer historyFile.Close()
	if err == nil {
		_, err = historyFile.Write(pipelineStateJson)
		if err == nil {
			_, err = historyFile.WriteString("\n")
		}
	}
	return err
}

func GetHistory(pipelineId string) ([]*common.PipelineState, error) {
	fileExists, err := checkFileExists(getPipelineStateHistoryFile(pipelineId))
	if err != nil {
		return nil, err
	}

	history_of_states := []*common.PipelineState{}

	if fileExists {
		fileBytes, readError := ioutil.ReadFile(getPipelineStateHistoryFile(pipelineId))

		if readError != nil {
			return nil, readError
		}
		var err error = nil
		decoder := json.NewDecoder(bytes.NewReader(fileBytes))
		for err == nil {
			var pipelineState common.PipelineState
			err = decoder.Decode(&pipelineState)
			if err == nil {
				history_of_states = append(history_of_states, &pipelineState)
			}
		}
		if err != io.EOF {
			return nil, err
		}
	}
	return history_of_states, nil
}

func getPipelineStateFile(pipelineId string) string {
	return getRunInfoDir(pipelineId) + PIPELINE_STATE_FILE
}

func getPipelineStateHistoryFile(pipelineId string) string {
	return getRunInfoDir(pipelineId) + PIPELINE_STATE_HISTORY_FILE
}

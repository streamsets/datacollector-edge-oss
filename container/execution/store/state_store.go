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
package store

import (
	"bytes"
	"encoding/json"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/util"
	"io"
	"io/ioutil"
	"os"
	"time"
)

const (
	PIPELINE_STATE_FILE         = "pipelineState.json"
	PIPELINE_STATE_HISTORY_FILE = "pipelineStateHistory.json"
	IS_REMOTE_PIPELINE          = "IS_REMOTE_PIPELINE"
	ISSUES                      = "issues"
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
			TimeStamp:  util.ConvertTimeToLong(time.Now()),
		}
		pipelineState.Attributes = make(map[string]interface{})
		pipelineState.Attributes[IS_REMOTE_PIPELINE] = false
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
		return &pipelineState, err
	}
}

func Edited(pipelineId string, isRemote bool) error {
	pipelineState, err := GetState(pipelineId)
	if err != nil {
		return err
	}

	if isRemote {
		pipelineState.Attributes = make(map[string]interface{})
		pipelineState.Attributes[IS_REMOTE_PIPELINE] = isRemote
	}

	return SaveState(pipelineId, pipelineState)
}

func SaveState(pipelineId string, pipelineState *common.PipelineState) error {
	var err error
	var pipelineStateJson []byte
	if pipelineStateJson, err = json.Marshal(pipelineState); err == nil {
		if err = ioutil.WriteFile(getPipelineStateFile(pipelineId), pipelineStateJson, 0644); err == nil {
			//open for append or create and open for write if it does not exist
			openFlag := os.O_APPEND | os.O_CREATE | os.O_WRONLY

			var historyFile *os.File

			//save in history file as well.
			if historyFile, err = os.OpenFile(getPipelineStateHistoryFile(pipelineId), openFlag, 0666); err == nil {
				defer util.CloseFile(historyFile)
				_, err = historyFile.Write(pipelineStateJson)
				if err == nil {
					_, err = historyFile.WriteString("\n")
				}
			}
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

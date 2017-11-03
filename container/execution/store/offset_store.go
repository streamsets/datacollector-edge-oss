/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package store

import (
	"encoding/json"
	"github.com/streamsets/datacollector-edge/container/common"
	"io/ioutil"
	"os"
)

var BaseDir = "."

const (
	OFFSET_FILE = "offset.json"
)

func GetOffset(pipelineId string) (common.SourceOffset, error) {
	defaultSourceOffset := common.GetDefaultOffset()
	if _, err := os.Stat(getPipelineOffsetFile(pipelineId)); os.IsNotExist(err) {
		return defaultSourceOffset, nil
	} else {
		file, readError := ioutil.ReadFile(getPipelineOffsetFile(pipelineId))

		if readError != nil {
			return defaultSourceOffset, readError
		}

		var sourceOffset common.SourceOffset
		json.Unmarshal(file, &sourceOffset)
		return sourceOffset, nil
	}
}

func SaveOffset(pipelineId string, sourceOffset common.SourceOffset) error {
	offsetJson, err := json.Marshal(sourceOffset)
	check(err)
	err1 := ioutil.WriteFile(getPipelineOffsetFile(pipelineId), offsetJson, 0644)
	return err1
}

func ResetOffset(pipelineId string) error {
	return SaveOffset(pipelineId, common.GetDefaultOffset())
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func getPipelineOffsetFile(pipelineId string) string {
	return getRunInfoDir(pipelineId) + OFFSET_FILE
}

func getRunInfoDir(pipelineId string) string {
	return BaseDir + "/data/runInfo/" + pipelineId + "/"
}

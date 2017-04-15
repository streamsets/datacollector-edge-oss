package store

import (
	"encoding/json"
	"github.com/streamsets/dataextractor/container/common"
	"io/ioutil"
	"os"
)

const (
	DEFAULT_OFFSET = ""
	OFFSET_FILE    = "offset.json"
)

func GetOffset() (common.SourceOffset, error) {
	defaultSourceOffset := common.SourceOffset{Version: 1, Offset: DEFAULT_OFFSET}
	if _, err := os.Stat(OFFSET_FILE); os.IsNotExist(err) {
		return defaultSourceOffset, nil
	} else {
		file, readError := ioutil.ReadFile(OFFSET_FILE)

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
	defaultSourceOffset := common.SourceOffset{Version: 1, Offset: DEFAULT_OFFSET}
	return SaveOffset(pipelineId, defaultSourceOffset)
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
	return "data/runInfo/" + pipelineId + "/"
}

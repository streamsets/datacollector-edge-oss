package store

import (
	"encoding/json"
	"github.com/streamsets/dataextractor/container/common"
	"io/ioutil"
	"os"
)

const (
	DEFAULT_OFFSET = ""
	OFFSET_FILE    = "data/offset.json"
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

func SaveOffset(sourceOffset common.SourceOffset) error {
	offsetJson, err := json.Marshal(sourceOffset)
	check(err)
	err1 := ioutil.WriteFile(OFFSET_FILE, offsetJson, 0644)
	return err1
}

func ResetOffset() error {
	defaultSourceOffset := common.SourceOffset{Version: 1, Offset: DEFAULT_OFFSET}
	return SaveOffset(defaultSourceOffset)
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

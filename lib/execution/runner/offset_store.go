package runner

import (
	"os"
	"io/ioutil"
	"encoding/json"
)

const (
	DEFAULT_OFFSET string = ""
	OFFSET_FILE = "data/offset.json"
)

func GetOffset() (*SourceOffset, error) {
	if _, err := os.Stat(OFFSET_FILE); os.IsNotExist(err) {
		return &SourceOffset{Version: 1, Offset: DEFAULT_OFFSET}, nil
	} else {
		file, readError := ioutil.ReadFile(OFFSET_FILE)

		if readError != nil {
			return nil, readError
		}

		var sourceOffset SourceOffset
		json.Unmarshal(file, &sourceOffset)
		return &sourceOffset, nil
	}
}

func SaveOffset(sourceOffset *SourceOffset) (error) {
	offsetJson, err := json.Marshal(sourceOffset)
	check(err)
	err1 := ioutil.WriteFile(OFFSET_FILE, offsetJson, 0644)
	return err1
}

func ResetOffset(sourceOffset *SourceOffset) (error) {
	sourceOffset.Offset = DEFAULT_OFFSET
	return SaveOffset(sourceOffset)
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

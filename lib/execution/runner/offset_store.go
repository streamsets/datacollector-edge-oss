package runner

import (
	"os"
	"io/ioutil"
	"encoding/json"
	"fmt"
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
	rankingsJson, err := json.Marshal(sourceOffset)
	check(err)
	fmt.Println(rankingsJson)
	err1 := ioutil.WriteFile(OFFSET_FILE, rankingsJson, 0644)
	return err1
}

func ResetOffset()  {
	SaveOffset(&SourceOffset{Version: 1, Offset: DEFAULT_OFFSET})
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

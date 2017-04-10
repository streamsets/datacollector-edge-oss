package filetail

import (
	"context"
	"fmt"
	"github.com/hpcloud/tail"
	"github.com/streamsets/dataextractor/api"
	"github.com/streamsets/dataextractor/container/common"
	"os"
	"strconv"
)

type FileTailOrigin struct {
	fileFullPath string
}

func (f *FileTailOrigin) Init(ctx context.Context) {
	fmt.Println("FileTailOrigin Init method: ")

	stageContext := (ctx.Value("stageContext")).(common.StageContext)
	stageConfig := stageContext.StageConfig
	for _, config := range stageConfig.Configuration {
		if config.Name == "conf.fileInfos" {
			fileInfos := config.Value.([]interface{})
			if len(fileInfos) > 0 {
				fileInfo := fileInfos[0].(map[string]interface{})
				f.fileFullPath = fileInfo["fileFullPath"].(string)
			}

		}
	}

	fmt.Println("Reading file - " + f.fileFullPath)
}

func (f *FileTailOrigin) Destroy() {
	fmt.Println("FileTailOrigin Destroy method")
}

func (f *FileTailOrigin) Produce(lastSourceOffset string, maxBatchSize int, batchMaker api.BatchMaker) (string, error) {
	tailConfig := tail.Config{Follow: true}

	if lastSourceOffset != "" {
		intOffset, _ := strconv.ParseInt(lastSourceOffset, 10, 64)
		tailConfig.Location = &tail.SeekInfo{Offset: intOffset, Whence: os.SEEK_SET}
	}

	tailObj, err := tail.TailFile(f.fileFullPath, tailConfig)

	if err != nil {
		fmt.Println("error:", err)
		panic(err)
	}

	recordCount := 0
	var offset int64
	for line := range tailObj.Lines {
		batchMaker.AddRecord(api.Record{Value: line.Text})
		recordCount++
		if recordCount > maxBatchSize {
			offset, _ = tailObj.Tell()
			break
		}
	}

	return strconv.FormatInt(offset, 10), err
}

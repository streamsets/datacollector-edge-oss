package filetail

import (
	"context"
	"fmt"
	"github.com/hpcloud/tail"
	"github.com/streamsets/dataextractor/api"
	"github.com/streamsets/dataextractor/container/common"
	"os"
	"strconv"
	"time"
)

type FileTailOrigin struct {
	fileFullPath    string
	maxWaitTimeSecs float64
}

func (f *FileTailOrigin) Init(ctx context.Context) {
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

		if config.Name == "conf.maxWaitTimeSecs" {
			f.maxWaitTimeSecs = config.Value.(float64)
		}
	}

	fmt.Println("Reading file - " + f.fileFullPath)
}

func (f *FileTailOrigin) Destroy() {
}

func (f *FileTailOrigin) Produce(lastSourceOffset string, maxBatchSize int, batchMaker api.BatchMaker) (string, error) {
	tailConfig := tail.Config{Follow: true, Logger: tail.DiscardingLogger}

	if lastSourceOffset != "" {
		intOffset, _ := strconv.ParseInt(lastSourceOffset, 10, 64)
		tailConfig.Location = &tail.SeekInfo{Offset: intOffset, Whence: os.SEEK_SET}
	}

	tailObj, err := tail.TailFile(f.fileFullPath, tailConfig)

	if err != nil {
		fmt.Println("error:", err)
		panic(err)
	}

	var currentOffset int64
	recordCount := 0
	end := false
	for !end {
		select {
		case line := <-tailObj.Lines:
			batchMaker.AddRecord(api.Record{Value: line.Text})
			recordCount++
			if recordCount > maxBatchSize {
				currentOffset, _ = tailObj.Tell()
				end = true
			}
		case <-time.After(time.Duration(f.maxWaitTimeSecs) * time.Second):
			currentOffset, _ = tailObj.Tell()
			end = true
		}
	}

	return strconv.FormatInt(currentOffset, 10), err
}

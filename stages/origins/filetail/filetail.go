package filetail

import (
	"context"
	"github.com/hpcloud/tail"
	"github.com/streamsets/dataextractor/api"
	"github.com/streamsets/dataextractor/container/common"
	"github.com/streamsets/dataextractor/stages/stagelibrary"
	"log"
	"os"
	"strconv"
	"time"
)

const (
	LIBRARY    = "streamsets-datacollector-basic-lib"
	STAGE_NAME = "com_streamsets_pipeline_stage_origin_logtail_FileTailDSource"
)

type FileTailOrigin struct {
	fileFullPath    string
	maxWaitTimeSecs float64
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &FileTailOrigin{}
	})
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

	log.Println("[DEBUG] Reading file - " + f.fileFullPath)
}

func (f *FileTailOrigin) Destroy() {
}

func (f *FileTailOrigin) Produce(lastSourceOffset string, maxBatchSize int, batchMaker api.BatchMaker) (string, error) {
	tailConfig := tail.Config{
		MustExist: true,
		Follow:    true,
		Logger:    tail.DiscardingLogger,
	}

	if lastSourceOffset != "" {
		intOffset, _ := strconv.ParseInt(lastSourceOffset, 10, 64)
		tailConfig.Location = &tail.SeekInfo{Offset: intOffset, Whence: os.SEEK_SET}
	}

	tailObj, err := tail.TailFile(f.fileFullPath, tailConfig)
	if err != nil {
		return lastSourceOffset, err
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

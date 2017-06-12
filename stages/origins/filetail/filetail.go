package filetail

import (
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
	LIBRARY                 = "streamsets-datacollector-basic-lib"
	STAGE_NAME              = "com_streamsets_pipeline_stage_origin_logtail_FileTailDSource"
	CONF_FILE_INFOS         = "conf.fileInfos"
	CONF_MAX_WAIT_TIME_SECS = "conf.maxWaitTimeSecs"
	CONF_FILE_FULL_PATH     = "fileFullPath"
)

type FileTailOrigin struct {
	*common.BaseStage
	fileFullPath    string
	maxWaitTimeSecs float64
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &FileTailOrigin{BaseStage: &common.BaseStage{}}
	})
}

func (f *FileTailOrigin) Init(stageContext api.StageContext) error {
	if err := f.BaseStage.Init(stageContext); err != nil {
		return err
	}
	stageConfig := f.GetStageConfig()
	for _, config := range stageConfig.Configuration {
		if config.Name == CONF_FILE_INFOS {
			fileInfos := config.Value.([]interface{})
			if len(fileInfos) > 0 {
				fileInfo := fileInfos[0].(map[string]interface{})
				f.fileFullPath = stageContext.GetResolvedValue(fileInfo[CONF_FILE_FULL_PATH]).(string)
			}

		}

		if config.Name == CONF_MAX_WAIT_TIME_SECS {
			f.maxWaitTimeSecs = stageContext.GetResolvedValue(config.Value).(float64)
		}
	}

	log.Println("[DEBUG] Reading file - " + f.fileFullPath)
	return nil
}

func (f *FileTailOrigin) Produce(lastSourceOffset string, maxBatchSize int, batchMaker api.BatchMaker) (string, error) {
	log.Println("[DEBUG] Last Source Offset : ", lastSourceOffset)

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
			if line != nil {
				batchMaker.AddRecord(
					f.GetStageContext().CreateRecord(
						tailObj.Filename+"::"+
							strconv.FormatInt(currentOffset, 10),
						line.Text))
				recordCount++
				if recordCount >= maxBatchSize {
					currentOffset, _ = tailObj.Tell()
					log.Println("[DEBUG] Calling stop for max record size")
					end = true
				}
			}
		case <-time.After(time.Duration(f.maxWaitTimeSecs) * time.Second):
			log.Println("[DEBUG] Calling stop for max Wait TimeSecs")
			currentOffset, _ = tailObj.Tell()
			end = true
		}
	}

	go f.stopTailing(tailObj)

	return strconv.FormatInt(currentOffset, 10), err
}

func (f *FileTailOrigin) stopTailing(tailObj *tail.Tail) {
	tailObj.Kill(nil)
	time.Sleep(time.Microsecond)

	end := false
	for !end {
		select {
		case _, ok := <-tailObj.Lines:
			if !ok {
				end = true
			}
		default:
			end = true
		}
	}

	tailObj.Wait()
}

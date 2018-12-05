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
package filetail

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/hpcloud/tail"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/recordio/delimitedrecord"
	"github.com/streamsets/datacollector-edge/container/util"
	"github.com/streamsets/datacollector-edge/stages/lib/dataparser"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	Library             = "streamsets-datacollector-basic-lib"
	StageName           = "com_streamsets_pipeline_stage_origin_logtail_FileTailDSource"
	ConfGroupFiles      = "FILES"
	ConfFileInfos       = "conf.fileInfos"
	ConfMaxWaitTimeSecs = "conf.maxWaitTimeSecs"
	ConfBatchSize       = "conf.batchSize"
	ConfDataFormat      = "conf.dataFormat"
	ErrorTail20         = "File path cannot be null or empty"
	ErrorTail02         = "File path doesn't exist: %s"
	Pattern             = "PATTERN"
	ELPattern           = "${PATTERN}"
	FileAttribute       = "file"
	FileNameAttribute   = "filename"
	MTimeAttribute      = "mtime"
)

type FileTailOrigin struct {
	*common.BaseStage
	Conf         FileTailConfigBean `ConfigDefBean:"name=conf"`
	fileTailList []*fileTail
	currentIndex int
}

type fileTail struct {
	fileFullPath string
	filename     string
	tailObj      *tail.Tail
	lastOffset   int64
	skippedLines int
	csvHeaders   []*api.Field
}

func (f *fileTail) getOffsetKey() string {
	return f.fileFullPath
}

type FileTailConfigBean struct {
	BatchSize        float64                           `ConfigDef:"type=NUMBER,required=true"`
	MaxWaitTimeSecs  float64                           `ConfigDef:"type=NUMBER,required=true"`
	FileInfos        []FileInfo                        `ConfigDef:"type=MODEL" ListBeanModel:"name=fileInfos"`
	DataFormat       string                            `ConfigDef:"type=STRING,required=true"`
	DataFormatConfig dataparser.DataParserFormatConfig `ConfigDefBean:"dataFormatConfig"`
}

type FileInfo struct {
	FileFullPath    string `ConfigDef:"type=STRING,required=true"`
	FileRollMode    string `ConfigDef:"type=STRING,required=true"`
	PatternForToken string `ConfigDef:"type=STRING,required=true"`
}

func init() {
	stagelibrary.SetCreator(Library, StageName, func() api.Stage {
		return &FileTailOrigin{BaseStage: &common.BaseStage{}}
	})
}

func (f *FileTailOrigin) Init(stageContext api.StageContext) []validation.Issue {
	issues := f.BaseStage.Init(stageContext)
	if len(f.Conf.FileInfos) == 0 || f.Conf.FileInfos[0].FileFullPath == "" {
		issues = append(issues, stageContext.CreateConfigIssue(ErrorTail20, ConfGroupFiles, ConfFileInfos))
		return issues
	}

	f.fileTailList = make([]*fileTail, 0)

	fileFullPathList := make([]string, 0)
	fileNameList := make([]string, 0)

	for _, fileInfo := range f.Conf.FileInfos {
		if fileInfo.FileFullPath == "" {
			issues = append(issues, stageContext.CreateConfigIssue(ErrorTail20, ConfGroupFiles, ConfFileInfos))
			return issues
		}
		filePaths, fileNames, err := getFilesPaths(fileInfo)
		if err != nil || len(filePaths) == 0 {
			issues = append(issues, stageContext.CreateConfigIssue(
				fmt.Sprintf(ErrorTail02, fileInfo.FileFullPath),
				ConfGroupFiles,
				ConfFileInfos,
			))
			return issues
		}
		fileFullPathList = append(fileFullPathList, filePaths...)
		fileNameList = append(fileNameList, fileNames...)
	}

	for i, fileFullPath := range fileFullPathList {
		log.WithField("file", fileFullPath).Debug("Reading file")

		fileTail := &fileTail{
			fileFullPath: fileFullPath,
			filename:     fileNameList[i],
		}

		if f.Conf.DataFormat == "DELIMITED" && f.Conf.DataFormatConfig.CsvHeader == delimitedrecord.WithHeader {
			file, _ := os.Open(fileFullPath)
			bufReader := bufio.NewReader(file)
			headerLine, err := bufReader.ReadString('\n')
			if err == nil {
				columns := strings.Split(headerLine, ",")
				fileTail.csvHeaders = make([]*api.Field, len(columns))
				for i, col := range columns {
					headerField, _ := api.CreateStringField(col)
					fileTail.csvHeaders[i] = headerField
				}
			}
			util.CloseFile(file)
		}

		f.fileTailList = append(f.fileTailList, fileTail)
	}

	return f.Conf.DataFormatConfig.Init(f.Conf.DataFormat, stageContext, issues)
}

func (f *FileTailOrigin) Produce(
	lastSourceOffset *string,
	maxBatchSize int,
	batchMaker api.BatchMaker,
) (*string, error) {
	var err error
	var offsetMap map[string]int64

	if offsetMap, err = f.reinitializeIfNeeded(lastSourceOffset); err != nil {
		log.WithError(err).Error("Failed to start tailing")
		return lastSourceOffset, err
	}

	batchSize := math.Min(float64(maxBatchSize), f.Conf.BatchSize)
	recordCount := float64(0)

	fileTailObj := f.fileTailList[f.currentIndex]

	timeout := time.NewTimer(time.Duration(f.Conf.MaxWaitTimeSecs) * time.Second)
	defer timeout.Stop()
	end := false
	for !end {
		select {
		case line := <-fileTailObj.tailObj.Lines:
			if line != nil {
				if line.Err != nil {
					log.WithError(line.Err).Errorf("error when tailing file: %s", fileTailObj.fileFullPath)
					f.GetStageContext().ReportError(err)
					break
				}
				if f.Conf.DataFormat == "DELIMITED" && lastSourceOffset == nil && recordCount == 0 {
					if fileTailObj.skippedLines < int(f.Conf.DataFormatConfig.CsvSkipStartLines) {
						fileTailObj.skippedLines++
						break
					} else if fileTailObj.skippedLines == 0 &&
						(f.Conf.DataFormatConfig.CsvHeader == delimitedrecord.WithHeader ||
							f.Conf.DataFormatConfig.CsvHeader == delimitedrecord.IgnoreHeader) {
						fileTailObj.skippedLines++
						break
					}
				}
				err = f.parseLine(line, batchMaker, &recordCount, fileTailObj)
				if err != nil {
					f.GetStageContext().ReportError(err)
				}

				if recordCount >= batchSize {
					end = true
				}
			}
		case <-timeout.C:
			end = true
		}
	}

	currentOffset, err := fileTailObj.tailObj.Tell()
	if err != nil {
		log.WithError(err).Error("Failed to get file offset information")
		f.GetStageContext().ReportError(err)
	}
	fileTailObj.lastOffset = currentOffset
	offsetMap[fileTailObj.getOffsetKey()] = currentOffset

	return f.serializeOffsetMap(offsetMap)
}

func (f *FileTailOrigin) Destroy() error {
	return f.stopAll()
}

func (f *FileTailOrigin) reinitializeIfNeeded(lastSourceOffset *string) (map[string]int64, error) {
	offsetMap, err := f.deserializeOffsetMap(lastSourceOffset)
	if err != nil {
		return offsetMap, err
	}

	stopRequired := false
	startRequired := false

	for _, fileTail := range f.fileTailList {
		if fileTail.tailObj == nil {
			fileTail.lastOffset = offsetMap[fileTail.getOffsetKey()]
			startRequired = true
		} else if fileTail.lastOffset != offsetMap[fileTail.getOffsetKey()] {
			log.WithField("old", fileTail.lastOffset).
				WithField("new", offsetMap[fileTail.getOffsetKey()]).
				Debug("Restart file tail because offset is different")
			fileTail.lastOffset = offsetMap[fileTail.getOffsetKey()]
			stopRequired = true
			startRequired = true
		}
	}

	if stopRequired {
		if err := f.stopAll(); err != nil {
			return offsetMap, err
		}
	}

	if startRequired {
		return offsetMap, f.startAll()
	} else {
		// Update round robin file tail
		f.currentIndex = (f.currentIndex + 1) % len(f.fileTailList)
	}

	return offsetMap, nil
}

func (f *FileTailOrigin) stopAll() error {
	log.Debug("Stopping all file tail process")
	var err error
	var wg sync.WaitGroup
	for _, fileTail := range f.fileTailList {
		if fileTail.tailObj != nil {
			wg.Add(1)
			go func(t *tail.Tail) {
				t.Kill(nil)
				wg.Done()
			}(fileTail.tailObj)
		}
	}
	wg.Wait()
	return err
}

func (f *FileTailOrigin) startAll() error {
	log.Debug("Starting all file tail process")
	var err error
	linesChannelList := make([]<-chan *tail.Line, len(f.fileTailList))

	for i, fileTail := range f.fileTailList {
		if err = f.startTailing(fileTail); err != nil {
			log.WithError(err).Errorf("Failed to stop File Tail Origin for file: %s", fileTail.fileFullPath)
			break
		}
		linesChannelList[i] = fileTail.tailObj.Lines
	}

	if err != nil {
		// if one of them failed to start, stop all to avoid any leakage
		_ = f.stopAll()
		return err
	}

	// start from first file
	f.currentIndex = 0
	return err
}

func (f *FileTailOrigin) startTailing(fileTail *fileTail) error {
	tailConfig := tail.Config{
		MustExist: true,
		Follow:    true,
		Logger:    tail.DiscardingLogger,
	}

	if fileTail.lastOffset > 0 {
		tailConfig.Location = &tail.SeekInfo{Offset: fileTail.lastOffset, Whence: io.SeekStart}
	}

	var err error
	fileTail.tailObj, err = tail.TailFile(fileTail.fileFullPath, tailConfig)
	if err != nil {
		return err
	}

	return err
}

func (f *FileTailOrigin) parseLine(
	line *tail.Line,
	batchMaker api.BatchMaker,
	recordCount *float64,
	fileTail *fileTail,
) error {
	sourceId := common.CreateRecordId("fileTail", int(*recordCount))
	record, err := f.Conf.DataFormatConfig.RecordCreator.CreateRecord(
		f.GetStageContext(),
		strings.Replace(line.Text, "\n", "", 1),
		sourceId,
		fileTail.csvHeaders,
	)
	if err != nil {
		f.GetStageContext().ReportError(err)
		return nil
	}

	record.GetHeader().SetAttribute(FileAttribute, fileTail.fileFullPath)
	record.GetHeader().SetAttribute(FileNameAttribute, fileTail.filename)
	record.GetHeader().SetAttribute(MTimeAttribute, fmt.Sprintf("%v", util.ConvertTimeToLong(line.Time)))

	batchMaker.AddRecord(record)
	*recordCount++
	return nil
}

func (f *FileTailOrigin) deserializeOffsetMap(lastSourceOffset *string) (map[string]int64, error) {
	offsetMap := make(map[string]int64)
	if lastSourceOffset == nil {
		offsetMap = make(map[string]int64)
	} else if strings.HasPrefix(*lastSourceOffset, "{") {
		// new format
		err := json.Unmarshal([]byte(*lastSourceOffset), &offsetMap)
		if err != nil {
			log.Error(err.Error())
			f.GetStageContext().ReportError(err)
			return offsetMap, err
		}
	} else {
		// old format
		intOffset, err := strconv.ParseInt(*lastSourceOffset, 10, 64)
		if len(f.fileTailList) > 0 && err == nil {
			offsetMap[f.fileTailList[0].getOffsetKey()] = intOffset
		}
	}
	return offsetMap, nil
}

func (f *FileTailOrigin) serializeOffsetMap(offsetMap map[string]int64) (*string, error) {
	b, err := json.Marshal(offsetMap)
	if err != nil {
		log.WithError(err).Error("Failed to get file offset information")
		f.GetStageContext().ReportError(err)
		return nil, err
	}

	lastSourceOffset := string(b)

	return &lastSourceOffset, nil
}

func getFilesPaths(fileInfo FileInfo) ([]string, []string, error) {
	fileFullPath := fileInfo.FileFullPath

	if fileInfo.FileRollMode == Pattern {
		fileFullPath = strings.Replace(fileFullPath, ELPattern, fileInfo.PatternForToken, -1)
	}

	allFilePaths, err := filepath.Glob(fileFullPath)
	if err != nil {
		return nil, nil, err
	}

	filePaths := make([]string, 0)
	fileNames := make([]string, 0)
	for _, filePath := range allFilePaths {
		if fileInfo, err := os.Stat(filePath); err == nil && !fileInfo.IsDir() {
			filePaths = append(filePaths, filePath)
			fileNames = append(fileNames, fileInfo.Name())
		}
	}

	return filePaths, fileNames, err
}

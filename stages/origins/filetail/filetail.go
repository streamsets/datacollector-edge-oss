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
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	Library                    = "streamsets-datacollector-basic-lib"
	StageName                  = "com_streamsets_pipeline_stage_origin_logtail_FileTailDSource"
	ConfGroupFiles             = "FILES"
	ConfFileInfos              = "conf.fileInfos"
	ConfMaxWaitTimeSecs        = "conf.maxWaitTimeSecs"
	ConfBatchSize              = "conf.batchSize"
	ConfDataFormat             = "conf.dataFormat"
	ErrorTail20                = "File path cannot be null or empty"
	ErrorTail02                = "File path doesn't exist: %s"
	ErrorTail08                = "The configuration for '%s' requires the '%s' token in the '%s' file name"
	FileRollModeReverseCounter = "REVERSE_COUNTER"
	FileRollModePattern        = "PATTERN"
	ELPattern                  = "${PATTERN}"
	FileAttribute              = "file"
	FileNameAttribute          = "filename"
	MTimeAttribute             = "mtime"
)

type FileTailOrigin struct {
	*common.BaseStage
	Conf                 FileTailConfigBean `ConfigDefBean:"name=conf"`
	currentFileInfoIndex int
	fileInfoRuntimeList  []*fileInfoRuntime
}

type fileInfoRuntime struct {
	fileTailList         []*fileTail
	currentFileTailIndex int
	fileInfo             FileInfo
}

type fileTail struct {
	fileFullPath   string
	filename       string
	key            string
	tailObj        *tail.Tail
	lastOffset     int64
	skippedLines   int
	lastBatchCount int64
	csvHeaders     []*api.Field
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
	FirstFile       string `ConfigDef:"type=STRING,required=true"`
}

func (f *FileInfo) getKey() string {
	return f.FileFullPath + "::" + f.PatternForToken
}

type offsetInfo struct {
	Offset   int64  `json:"offset"`
	FileName string `json:"fileName"`
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

	for _, fileInfo := range f.Conf.FileInfos {
		issues = f.validateFileInfo(fileInfo, issues)
	}

	if len(issues) > 0 {
		return issues
	}

	f.fileInfoRuntimeList = make([]*fileInfoRuntime, len(f.Conf.FileInfos))
	for j, fileInfo := range f.Conf.FileInfos {
		f.fileInfoRuntimeList[j] = &fileInfoRuntime{fileInfo: fileInfo}
		f.fileInfoRuntimeList[j].fileTailList = make([]*fileTail, 0)

		if fileInfo.FileRollMode == FileRollModePattern {
			dirPaths, err := getDirectoryPaths(fileInfo)
			if err != nil || len(dirPaths) == 0 {
				issues = append(issues, stageContext.CreateConfigIssue(
					fmt.Sprintf(ErrorTail02, fileInfo.FileFullPath),
					ConfGroupFiles,
					ConfFileInfos,
				))
				return issues
			}

			for _, dirPath := range dirPaths {
				log.WithField("file", dirPath).Debug("Reading file")

				nextFileName, err := f.getPatternNextFile(fileInfo, dirPath, "")
				if err != nil {
					log.WithError(err).Error("Failed to get next pattern file")
				}

				fileTail := &fileTail{
					fileFullPath: filepath.Join(dirPath, nextFileName),
					filename:     nextFileName,
					key:          dirPath,
				}

				f.fileInfoRuntimeList[j].fileTailList = append(f.fileInfoRuntimeList[j].fileTailList, fileTail)
			}
		} else {
			filePaths, fileNames, err := getFilesPaths(fileInfo)
			if err != nil || len(filePaths) == 0 {
				issues = append(issues, stageContext.CreateConfigIssue(
					fmt.Sprintf(ErrorTail02, fileInfo.FileFullPath),
					ConfGroupFiles,
					ConfFileInfos,
				))
				return issues
			}

			for i, fileFullPath := range filePaths {
				log.WithField("file", fileFullPath).Debug("Reading file")

				fileTail := &fileTail{
					fileFullPath: fileFullPath,
					filename:     fileNames[i],
					key:          fileFullPath,
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

				f.fileInfoRuntimeList[j].fileTailList = append(f.fileInfoRuntimeList[j].fileTailList, fileTail)
			}
		}
	}

	return f.Conf.DataFormatConfig.Init(f.Conf.DataFormat, stageContext, issues)
}

func (f *FileTailOrigin) Produce(
	lastSourceOffset *string,
	maxBatchSize int,
	batchMaker api.BatchMaker,
) (*string, error) {
	var err error
	var offsetMap map[string]map[string]offsetInfo

	if offsetMap, err = f.reinitializeIfNeeded(lastSourceOffset); err != nil {
		log.WithError(err).Error("Failed to start tailing")
		f.GetStageContext().ReportError(err)
		return lastSourceOffset, nil
	}

	batchSize := math.Min(float64(maxBatchSize), f.Conf.BatchSize)
	recordCount := float64(0)

	fileInfoRuntime := f.fileInfoRuntimeList[f.currentFileInfoIndex]
	fileTailObj := fileInfoRuntime.fileTailList[fileInfoRuntime.currentFileTailIndex]

	log.WithField("filepath", fileTailObj.fileFullPath).Debug("In Produce method")

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
	fileTailObj.lastBatchCount = int64(recordCount)
	offsetMap[fileInfoRuntime.fileInfo.getKey()][fileTailObj.key] = offsetInfo{currentOffset, fileTailObj.filename}

	return f.serializeOffsetMap(offsetMap)
}

func (f *FileTailOrigin) Destroy() error {
	return f.stopAll()
}

func (f *FileTailOrigin) reinitializeIfNeeded(lastSourceOffset *string) (map[string]map[string]offsetInfo, error) {
	offsetMap, err := f.deserializeOffsetMap(lastSourceOffset)
	if err != nil {
		return offsetMap, err
	}

	stopRequired := false
	startRequired := false
	resetCurrentIndex := false
	incrementIndex := true

	for _, fileInfoRuntime := range f.fileInfoRuntimeList {
		fileInfoOffsetMap := offsetMap[fileInfoRuntime.fileInfo.getKey()]
		for _, fileTail := range fileInfoRuntime.fileTailList {
			if fileTail.tailObj == nil {
				fileTail.lastOffset = fileInfoOffsetMap[fileTail.key].Offset
				startRequired = true
				resetCurrentIndex = true
				incrementIndex = false
			} else if fileTail.lastOffset != fileInfoOffsetMap[fileTail.key].Offset || fileTail.filename != fileInfoOffsetMap[fileTail.key].FileName {
				log.WithField("old", fileTail.lastOffset).
					WithField("new", fileInfoOffsetMap[fileTail.key].Offset).
					Debug("Restart file tail because offset is different")

				log.WithField("old", fileTail.filename).
					WithField("new", fileInfoOffsetMap[fileTail.key].FileName).
					Debug("Restart file tail because offset is different")

				fileTail.lastOffset = fileInfoOffsetMap[fileTail.key].Offset
				fileTail.filename = fileInfoOffsetMap[fileTail.key].FileName
				stopRequired = true
				startRequired = true
				resetCurrentIndex = true
				incrementIndex = false
			} else if fileInfoRuntime.fileInfo.FileRollMode == FileRollModePattern && fileTail.lastOffset > 0 && fileTail.lastBatchCount == 0 {
				// for pattern roll mode
				dirPath := filepath.Dir(fileTail.fileFullPath)

				nextFileName, err := f.getPatternNextFile(fileInfoRuntime.fileInfo, dirPath, fileTail.filename)
				if err != nil {
					log.WithError(err).Error("Failed to get next pattern file")
				}

				if nextFileName != fileTail.filename {
					fileTail.fileFullPath = filepath.Join(dirPath, nextFileName)
					fileTail.filename = nextFileName
					fileTail.lastOffset = 0
					fileTail.lastBatchCount = 0

					fileInfoOffsetMap[fileTail.key] = offsetInfo{
						FileName: nextFileName,
						Offset:   0,
					}

					log.Debugf("Rolling to new file: %s", fileTail.fileFullPath)

					stopRequired = true
					startRequired = true
					incrementIndex = false
					break
				}
			}
		}
	}

	if stopRequired {
		if err := f.stopAll(); err != nil {
			return offsetMap, err
		}
	}

	if startRequired {
		if err := f.startAll(resetCurrentIndex); err != nil {
			return offsetMap, err
		}
	}

	if incrementIndex {
		// Update round robin file tail
		f.currentFileInfoIndex = (f.currentFileInfoIndex + 1) % len(f.Conf.FileInfos)
		fileInfoRuntime := f.fileInfoRuntimeList[f.currentFileInfoIndex]
		fileInfoRuntime.currentFileTailIndex = (fileInfoRuntime.currentFileTailIndex + 1) % len(fileInfoRuntime.fileTailList)
	}

	return offsetMap, nil
}

func (f *FileTailOrigin) stopAll() error {
	log.Info("Stopping all file tail process")
	var err error
	var wg sync.WaitGroup
	for _, fileInfoRuntime := range f.fileInfoRuntimeList {
		for _, fileTail := range fileInfoRuntime.fileTailList {
			if fileTail.tailObj != nil {
				wg.Add(1)
				go func(t *tail.Tail) {
					t.Kill(nil)
					wg.Done()
				}(fileTail.tailObj)
			}
		}
	}
	wg.Wait()
	return err
}

func (f *FileTailOrigin) startAll(resetCurrentIndex bool) error {
	log.Info("Starting all file tail process")
	var err error

	for _, fileInfoRuntime := range f.fileInfoRuntimeList {
		for _, fileTail := range fileInfoRuntime.fileTailList {
			if err = f.startTailing(fileTail); err != nil {
				log.WithError(err).Errorf("Failed to stop File Tail Origin for file: %s", fileTail.fileFullPath)
				break
			}
		}
		if resetCurrentIndex {
			fileInfoRuntime.currentFileTailIndex = 0
		}
	}

	if err != nil {
		// if one of them failed to start, stop all to avoid any leakage
		_ = f.stopAll()
		return err
	}

	if resetCurrentIndex {
		// start from first file
		f.currentFileInfoIndex = 0
	}

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
		strings.TrimRight(line.Text, "\r\n"),
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

func (f *FileTailOrigin) deserializeOffsetMap(lastSourceOffset *string) (map[string]map[string]offsetInfo, error) {
	offsetMap := make(map[string]map[string]offsetInfo)

	if lastSourceOffset == nil || *lastSourceOffset == "" {
		for _, fileInfoRuntime := range f.fileInfoRuntimeList {
			offsetMap[fileInfoRuntime.fileInfo.getKey()] = make(map[string]offsetInfo)
			for _, fileTail := range fileInfoRuntime.fileTailList {
				offsetMap[fileInfoRuntime.fileInfo.getKey()][fileTail.key] = offsetInfo{FileName: fileTail.filename}
			}
		}
		return offsetMap, nil
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
		for _, fileInfoRuntime := range f.fileInfoRuntimeList {
			offsetMap[fileInfoRuntime.fileInfo.getKey()] = make(map[string]offsetInfo)
		}
		intOffset, err := strconv.ParseInt(*lastSourceOffset, 10, 64)
		if len(f.fileInfoRuntimeList) > 0 && len(f.fileInfoRuntimeList[0].fileTailList) > 0 && err == nil {
			offsetMap[f.Conf.FileInfos[0].getKey()][f.fileInfoRuntimeList[0].fileTailList[0].key] =
				offsetInfo{Offset: intOffset, FileName: f.Conf.FileInfos[0].FileFullPath}
		}
	}
	return offsetMap, nil
}

func (f *FileTailOrigin) serializeOffsetMap(offsetMap map[string]map[string]offsetInfo) (*string, error) {
	b, err := json.Marshal(offsetMap)
	if err != nil {
		log.WithError(err).Error("Failed to get file offset information")
		f.GetStageContext().ReportError(err)
		return nil, err
	}

	lastSourceOffset := string(b)
	return &lastSourceOffset, nil
}

func getDirectoryPaths(fileInfo FileInfo) ([]string, error) {
	fileFullPath := fileInfo.FileFullPath
	fileFullPath = strings.Replace(fileFullPath, ELPattern, "*", -1)

	allDirPaths, err := filepath.Glob(filepath.Dir(fileFullPath))
	if err != nil {
		return nil, err
	}

	dirPaths := make([]string, 0)

	for _, dirPath := range allDirPaths {
		if fileInfo, err := os.Stat(dirPath); err == nil && fileInfo.IsDir() {
			dirPaths = append(dirPaths, dirPath)
		}
	}
	return dirPaths, nil
}

func getFilesPaths(fileInfo FileInfo) ([]string, []string, error) {
	allFilePaths, err := filepath.Glob(fileInfo.FileFullPath)
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

	return filePaths, fileNames, nil
}

func (f *FileTailOrigin) validateFileInfo(fileInfo FileInfo, issues []validation.Issue) []validation.Issue {
	if fileInfo.FileFullPath == "" {
		issues = append(issues, f.GetStageContext().CreateConfigIssue(ErrorTail20, ConfGroupFiles, ConfFileInfos))
		return issues
	}

	if fileInfo.FileRollMode == FileRollModePattern && len(fileInfo.PatternForToken) == 0 {
		issues = append(issues, f.GetStageContext().CreateConfigIssue(
			fmt.Sprintf(ErrorTail08, fileInfo.FileFullPath, ELPattern, filepath.Base(fileInfo.FileFullPath)),
			ConfGroupFiles,
			ConfFileInfos,
		))
		return issues
	}

	return issues
}

func (f *FileTailOrigin) getPatternNextFile(fileInfo FileInfo, dirPath string, currentFileName string) (string, error) {
	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return "", err
	}

	allFileNames := make([]string, len(files))
	for i, f := range files {
		allFileNames[i] = f.Name()
	}

	sort.Strings(allFileNames)
	if err != nil {
		return "", err
	}

	if len(currentFileName) == 0 {
		if len(fileInfo.FirstFile) != 0 {
			if _, err := os.Stat(filepath.Join(dirPath, fileInfo.FirstFile)); os.IsNotExist(err) {
				return allFileNames[len(allFileNames)-1], nil
			} else if len(allFileNames) > 0 {
				return fileInfo.FirstFile, nil
			}
		} else {
			return allFileNames[0], nil
		}
	}

	if currentIndex := util.IndexOf(currentFileName, allFileNames); currentIndex != -1 && (currentIndex+1) < len(allFileNames) {
		return allFileNames[currentIndex+1], nil
	}

	return currentFileName, nil
}

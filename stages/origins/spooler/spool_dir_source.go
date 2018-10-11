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
package spooler

import (
	"bufio"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/recordio"
	"github.com/streamsets/datacollector-edge/container/recordio/delimitedrecord"
	"github.com/streamsets/datacollector-edge/stages/lib/dataparser"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	Library         = "streamsets-datacollector-basic-lib"
	StageName       = "com_streamsets_pipeline_stage_origin_spooldir_SpoolDirDSource"
	Timestamp       = "TIMESTAMP"
	Lexicographical = "LEXICOGRAPHICAL"
	EOFOffset       = int64(-1)
	InvalidOffset   = int64(-2)
	File            = "file"
	FileName        = "filename"
	Offset          = "offset"
	Glob            = "GLOB"
	Regex           = "REGEX"
)

type SpoolDirSource struct {
	*common.BaseStage
	Conf       SpoolDirConfigBean `ConfigDefBean:"conf"`
	spooler    *DirectorySpooler
	bufReader  *bufio.Reader
	file       *os.File
	csvHeaders []*api.Field
}

type SpoolDirConfigBean struct {
	SpoolDir              string                            `ConfigDef:"type=STRING,required=true"`
	UseLastModified       string                            `ConfigDef:"type=STRING,required=true"`
	PoolingTimeoutSecs    float64                           `ConfigDef:"type=NUMBER,required=true"`
	InitialFileToProcess  string                            `ConfigDef:"type=STRING,required=true"`
	ProcessSubdirectories bool                              `ConfigDef:"type=BOOLEAN,required=true"`
	FilePattern           string                            `ConfigDef:"type=STRING,required=true"`
	PathMatcherMode       string                            `ConfigDef:"type=STRING,required=true"`
	DataFormat            string                            `ConfigDef:"type=STRING,required=true"`
	DataFormatConfig      dataparser.DataParserFormatConfig `ConfigDefBean:"dataFormatConfig"`
}

func init() {
	stagelibrary.SetCreator(Library, StageName, func() api.Stage {
		return &SpoolDirSource{BaseStage: &common.BaseStage{}}
	})
}

func (s *SpoolDirSource) Init(stageContext api.StageContext) []validation.Issue {
	issues := s.BaseStage.Init(stageContext)
	s.spooler = &DirectorySpooler{
		dirPath:           s.Conf.SpoolDir,
		readOrder:         s.Conf.UseLastModified,
		pathMatcherMode:   s.Conf.PathMatcherMode,
		filePattern:       s.Conf.FilePattern,
		processSubDirs:    s.Conf.ProcessSubdirectories,
		spoolWaitDuration: time.Duration(int64(s.Conf.PoolingTimeoutSecs) * 1000 * 1000),
	}

	if s.spooler.pathMatcherMode != Glob && s.spooler.pathMatcherMode != Regex {
		issues = append(issues, stageContext.CreateConfigIssue(
			"Unsupported Path Matcher mode :"+s.spooler.pathMatcherMode,
		))
		return issues
	}

	s.spooler.Init()
	if s.Conf.InitialFileToProcess != "" {
		fileMatches, err := filepath.Glob(s.Conf.InitialFileToProcess)
		if err == nil {
			if len(fileMatches) > 1 {
				issues = append(issues, stageContext.CreateConfigIssue(
					"Initial File to Process '"+
						s.Conf.InitialFileToProcess+"' matches multiple files",
				))
				return issues
			}
			s.Conf.InitialFileToProcess = fileMatches[0]
		}
	}
	return s.Conf.DataFormatConfig.Init(s.Conf.DataFormat, stageContext, issues)
}

func (s *SpoolDirSource) initializeBuffReaderIfNeeded() error {
	fInfo := s.spooler.getCurrentFileInfo()
	if s.file == nil {
		f, err := os.Open(fInfo.getFullPath())
		if err != nil {
			return err
		}
		s.file = f

		if s.Conf.DataFormat == "DELIMITED" && s.Conf.DataFormatConfig.CsvHeader == delimitedrecord.WithHeader &&
			(len(s.csvHeaders) == 0 || fInfo.getOffsetToRead() == 0) {
			bufReader := bufio.NewReader(s.file)
			headerLine, err := bufReader.ReadString('\n')
			if err == nil {
				columns := strings.Split(headerLine, ",")
				s.csvHeaders = make([]*api.Field, len(columns))
				for i, col := range columns {
					headerField, _ := api.CreateStringField(col)
					s.csvHeaders[i] = headerField
				}
			}
		}

		if _, err := s.file.Seek(fInfo.getOffsetToRead(), 0); err != nil {
			return err
		}
		s.bufReader = bufio.NewReader(s.file)

		if s.Conf.DataFormat == "DELIMITED" && fInfo.getOffsetToRead() == 0 {
			bytesRead := 0
			if s.Conf.DataFormatConfig.CsvSkipStartLines > 0 {
				skippedLines := 0
				for skippedLines < int(s.Conf.DataFormatConfig.CsvSkipStartLines) {
					lineBytes, err := s.bufReader.ReadBytes('\n')
					if err == nil {
						bytesRead += len(lineBytes)
					}
					skippedLines++
				}
			} else if s.Conf.DataFormatConfig.CsvHeader == delimitedrecord.WithHeader ||
				s.Conf.DataFormatConfig.CsvHeader == delimitedrecord.IgnoreHeader {
				lineBytes, err := s.bufReader.ReadBytes('\n')
				if err == nil {
					bytesRead += len(lineBytes)
				}
			}
			s.spooler.getCurrentFileInfo().incOffsetToRead(int64(bytesRead))
		}
	}
	return nil
}

func (s *SpoolDirSource) initCurrentFileIfNeeded(lastSourceOffset *string) (bool, error) {
	currentFilePath, currentStartOffset, modTime, err := parseLastOffset(*lastSourceOffset)

	if err != nil {
		return false, err
	}

	// Pipeline resume case
	if s.spooler.getCurrentFileInfo() == nil && currentFilePath != "" {
		s.spooler.setCurrentFileInfo(
			NewAtomicFileInformation(
				currentFilePath,
				modTime,
				currentStartOffset,
			),
		)
	}

	// Offset is not present and initial file to process is configured.
	if currentFilePath == "" && s.Conf.InitialFileToProcess != "" {
		fileInfo, err := os.Stat(currentFilePath)
		if err != nil {
			return false, err
		}
		s.spooler.setCurrentFileInfo(
			NewAtomicFileInformation(
				s.Conf.InitialFileToProcess,
				fileInfo.ModTime(),
				currentStartOffset,
			),
		)
		log.WithField("File Name", currentFilePath).Debug("Using Initial File To Process")
	}

	//End of the file or empty offset, let's get a new file
	if currentFilePath == "" || currentStartOffset == -1 {
		nextFileInfoToProcess := s.spooler.NextFile()
		//No more files to process at the moment
		if nextFileInfoToProcess == nil {
			log.Debug("No more files to process")
			return false, nil
		}
	}
	return true, nil
}

func (s *SpoolDirSource) createRecordAndAddToBatch(
	recordReaderFactory recordio.RecordReaderFactory,
	lineText string,
	batchMaker api.BatchMaker,
) error {
	fInfo := s.spooler.getCurrentFileInfo()
	recordId := fInfo.getFullPath() + "::" + strconv.FormatInt(fInfo.getOffsetToRead(), 10)
	record, err := recordReaderFactory.CreateRecord(
		s.GetStageContext(),
		lineText,
		recordId,
		s.csvHeaders,
	)
	if err != nil {
		s.GetStageContext().ReportError(err)
		return nil
	}
	record.GetHeader().SetAttribute(File, fInfo.getFullPath())
	record.GetHeader().SetAttribute(FileName, fInfo.getName())
	record.GetHeader().SetAttribute(
		Offset,
		strconv.FormatInt(fInfo.getOffsetToRead(), 10),
	)
	batchMaker.AddRecord(record)

	return nil
}

func (s *SpoolDirSource) readAndCreateRecords(
	maxBatchSize int,
	batchMaker api.BatchMaker,
) (int64, error) {
	isEof := false
	startOffsetForBatch := s.spooler.getCurrentFileInfo().getOffsetToRead()
	recordReaderFactory := s.Conf.DataFormatConfig.RecordReaderFactory
	for recordCnt := 0; recordCnt < maxBatchSize; recordCnt++ {
		if s.bufReader == nil {
			// if pipeline stopped
			break
		}
		lineBytes, err := s.bufReader.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				// TODO Try Error Archiving for file?
				log.WithError(err).Error("Error while reading file")
				s.GetStageContext().ReportError(err)
				return startOffsetForBatch, nil
			}
			isEof = true
		}

		bytesRead := len(lineBytes)
		if bytesRead > 0 {
			err = s.createRecordAndAddToBatch(
				recordReaderFactory,
				strings.Replace(string(lineBytes), "\n", "", 1),
				batchMaker,
			)

			if err != nil {
				// TODO Try Error Archiving for file?
				log.WithError(err).Error("Error while reading file")
				s.GetStageContext().ReportError(err)
				return startOffsetForBatch, nil
			}
		}

		if isEof {
			log.WithField("File Name", s.spooler.getCurrentFileInfo().getFullPath()).
				Debug("Reached End of File")
			s.spooler.getCurrentFileInfo().setOffsetToRead(EOFOffset)
			s.resetFileAndBuffReader()
			break
		}
		s.spooler.getCurrentFileInfo().incOffsetToRead(int64(bytesRead))
	}

	return s.spooler.getCurrentFileInfo().getOffsetToRead(), nil
}

func parseLastOffset(offsetString string) (string, int64, time.Time, error) {
	if offsetString == "" {
		return "", InvalidOffset, time.Now(), nil
	}
	offsetSplit := strings.Split(offsetString, "::")

	filePath := offsetSplit[0]

	offsetInFile, err := strconv.ParseInt(offsetSplit[1], 10, 64)

	modTimeNano, err := strconv.ParseInt(offsetSplit[2], 10, 64)

	return filePath, offsetInFile, time.Unix(0, modTimeNano), err
}

func (s *SpoolDirSource) Produce(
	lastSourceOffset *string,
	maxBatchSize int,
	batchMaker api.BatchMaker,
) (*string, error) {

	shouldProduce, err := s.initCurrentFileIfNeeded(lastSourceOffset)

	if err != nil {
		s.GetStageContext().ReportError(err)
		log.WithError(err).Error("Error occurred")
		return lastSourceOffset, err
	}

	if shouldProduce {
		if s.spooler.getCurrentFileInfo() == nil {
			return lastSourceOffset, nil
		}

		err = s.initializeBuffReaderIfNeeded()

		if err != nil {
			s.GetStageContext().ReportError(err)
			return lastSourceOffset, err
		}

		offset, err := s.readAndCreateRecords(maxBatchSize, batchMaker)

		if offset == InvalidOffset && err != nil {
			s.GetStageContext().ReportError(err)
			return lastSourceOffset, err
		}
		newOffset := s.spooler.getCurrentFileInfo().createOffset()
		return &newOffset, err
	}
	return lastSourceOffset, err
}

func (s *SpoolDirSource) resetFileAndBuffReader() {
	if s.file != nil {
		// Close Quietly
		if err := s.file.Close(); err != nil {
			log.WithError(err).WithField("file", s.file.Name()).Error("Error During file close")
		}
		s.file = nil
	}
	s.bufReader = nil
}

func (s *SpoolDirSource) Destroy() error {
	s.resetFileAndBuffReader()
	s.spooler.Destroy()
	return nil
}

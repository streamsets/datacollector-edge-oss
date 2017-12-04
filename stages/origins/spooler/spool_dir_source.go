/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package spooler

import (
	"bufio"
	"errors"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	LIBRARY         = "streamsets-datacollector-basic-lib"
	STAGE_NAME      = "com_streamsets_pipeline_stage_origin_spooldir_SpoolDirDSource"
	LAST_MODIFIED   = "LAST_MODIFIED"
	LEXICOGRAPHICAL = "LEXICOGRAPHICAL"
	EOF_OFFSET      = int64(-1)
	INVALID_OFFSET  = int64(-2)

	SPOOL_DIR_PATH          = "conf.spoolDir"
	USE_LAST_MODIFIED       = "conf.useLastModified"
	POLLING_TIMEOUT_SECONDS = "conf.poolingTimeoutSecs"
	INITIAL_FILE_TO_PROCESS = "conf.initialFileToProcess"
	PROCESS_SUB_DIRECTORIES = "conf.processSubdirectories"
	FILE_PATTERN            = "conf.filePattern"
	PATH_MATHER_MODE        = "conf.pathMatcherMode"

	FILE      = "file"
	FILE_NAME = "filename"
	OFFSET    = "offset"
	GLOB      = "GLOB"
	REGEX     = "REGEX"
)

type SpoolDirSource struct {
	*common.BaseStage
	Conf      SpoolDirConfigBean `ConfigDefBean:"conf"`
	spooler   *DirectorySpooler
	bufReader *bufio.Reader
	file      *os.File
}

type SpoolDirConfigBean struct {
	SpoolDir              string  `ConfigDef:"type=STRING,required=true"`
	UseLastModified       string  `ConfigDef:"type=STRING,required=true"`
	PoolingTimeoutSecs    float64 `ConfigDef:"type=NUMBER,required=true"`
	InitialFileToProcess  string  `ConfigDef:"type=STRING,required=true"`
	ProcessSubdirectories bool    `ConfigDef:"type=BOOLEAN,required=true"`
	FilePattern           string  `ConfigDef:"type=STRING,required=true"`
	PathMatcherMode       string  `ConfigDef:"type=STRING,required=true"`
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &SpoolDirSource{BaseStage: &common.BaseStage{}}
	})
}

func (s *SpoolDirSource) Init(stageContext api.StageContext) error {
	if err := s.BaseStage.Init(stageContext); err != nil {
		return err
	}
	s.spooler = &DirectorySpooler{
		dirPath:           s.Conf.SpoolDir,
		readOrder:         s.Conf.UseLastModified,
		pathMatcherMode:   s.Conf.PathMatcherMode,
		filePattern:       s.Conf.FilePattern,
		processSubDirs:    s.Conf.ProcessSubdirectories,
		spoolWaitDuration: time.Duration(int64(s.Conf.PoolingTimeoutSecs) * 1000 * 1000),
	}

	if s.spooler.pathMatcherMode != GLOB && s.spooler.pathMatcherMode != REGEX {
		return errors.New("Unsupported Path Matcher mode :" + s.spooler.pathMatcherMode)
	}

	s.spooler.Init()
	var err error = nil
	if s.Conf.InitialFileToProcess != "" {
		file_matches, err := filepath.Glob(s.Conf.InitialFileToProcess)
		if err == nil {
			if len(file_matches) > 1 {
				return errors.New(
					"Initial File to Process '" +
						s.Conf.InitialFileToProcess + "' matches multiple files",
				)
			}
			s.Conf.InitialFileToProcess = file_matches[0]
		}
	}
	return err
}

func (s *SpoolDirSource) initializeBuffReaderIfNeeded() error {
	fInfo := s.spooler.getCurrentFileInfo()
	if s.file == nil {
		f, err := os.Open(fInfo.getFullPath())
		if err != nil {
			return err
		}
		s.file = f
		if _, err := s.file.Seek(fInfo.getOffsetToRead(), 0); err != nil {
			return err
		}
		s.bufReader = bufio.NewReader(s.file)
	}
	return nil
}

func (s *SpoolDirSource) initCurrentFileIfNeeded(lastSourceOffset string) (bool, error) {
	currentFilePath, currentStartOffset, modTime, err := parseLastOffset(lastSourceOffset)

	if err != nil {
		return false, err
	}

	//Pipeline resume case
	if s.spooler.getCurrentFileInfo() == nil && currentFilePath != "" {
		s.spooler.setCurrentFileInfo(
			NewAtomicFileInformation(
				currentFilePath,
				modTime,
				currentStartOffset,
			),
		)
	}

	//Offset is not present and initial file to process is configured.
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
		log.WithField("file", currentFilePath).Debug("Using Initial File To Process")
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

func (s *SpoolDirSource) createRecordAndAddToBatch(line_bytes []byte, batchMaker api.BatchMaker) {
	fInfo := s.spooler.getCurrentFileInfo()
	if len(line_bytes) > 0 {
		if line_bytes[len(line_bytes)-1] == byte('\n') {
			line_bytes = line_bytes[:len(line_bytes)-1] //Throwing out delimiter
		}

		record, _ := s.GetStageContext().CreateRecord(
			fInfo.getFullPath()+"::"+
				strconv.FormatInt(fInfo.getOffsetToRead(), 10),
			string(line_bytes),
		)

		record.GetHeader().SetAttribute(FILE, fInfo.getFullPath())
		record.GetHeader().SetAttribute(FILE_NAME, fInfo.getName())
		record.GetHeader().SetAttribute(
			OFFSET,
			strconv.FormatInt(fInfo.getOffsetToRead(), 10),
		)

		batchMaker.AddRecord(record)
	}
}

func (s *SpoolDirSource) readAndCreateRecords(
	maxBatchSize int,
	batchMaker api.BatchMaker,
) (int64, error) {
	isEof := false

	startOffsetForBatch := s.spooler.getCurrentFileInfo().getOffsetToRead()

	for recordCnt := 0; recordCnt < maxBatchSize; recordCnt++ {
		line_bytes, err := s.bufReader.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				//TODO Try Error Archiving for file?
				log.WithError(err).Error("Error while reading file")
				return startOffsetForBatch, err
			}
			isEof = true
		}
		bytesRead := len(line_bytes)

		s.createRecordAndAddToBatch(line_bytes, batchMaker)

		if isEof {
			log.WithField("file", s.spooler.getCurrentFileInfo().getFullPath()).Debug("Reached End of File")
			s.spooler.getCurrentFileInfo().setOffsetToRead(EOF_OFFSET)
			s.resetFileAndBuffReader()
			break
		}
		s.spooler.getCurrentFileInfo().incOffsetToRead(int64(bytesRead))
	}

	return s.spooler.getCurrentFileInfo().getOffsetToRead(), nil
}

func parseLastOffset(offsetString string) (string, int64, time.Time, error) {
	if offsetString == "" {
		return "", INVALID_OFFSET, time.Now(), nil
	}
	offsetSplit := strings.Split(offsetString, "::")

	filePath := offsetSplit[0]

	offsetInFile, err := strconv.ParseInt(offsetSplit[1], 10, 64)

	modTimeNano, err := strconv.ParseInt(offsetSplit[2], 10, 64)

	return filePath, offsetInFile, time.Unix(0, modTimeNano), err
}

func (s *SpoolDirSource) Produce(
	lastSourceOffset string,
	maxBatchSize int,
	batchMaker api.BatchMaker,
) (string, error) {

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

		if offset == INVALID_OFFSET && err != nil {
			s.GetStageContext().ReportError(err)
			return lastSourceOffset, err
		}
		return s.spooler.getCurrentFileInfo().createOffset(), nil
	}
	return lastSourceOffset, err
}

func (s *SpoolDirSource) resetFileAndBuffReader() {
	if s.file != nil {
		//Close Quietly
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

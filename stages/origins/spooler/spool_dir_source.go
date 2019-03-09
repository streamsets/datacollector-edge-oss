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
	"compress/gzip"
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
	Library             = "streamsets-datacollector-basic-lib"
	StageName           = "com_streamsets_pipeline_stage_origin_spooldir_SpoolDirDSource"
	Timestamp           = "TIMESTAMP"
	Lexicographical     = "LEXICOGRAPHICAL"
	EOFOffset           = int64(-1)
	InvalidOffset       = int64(-2)
	File                = "file"
	FileName            = "filename"
	Offset              = "offset"
	Glob                = "GLOB"
	Regex               = "REGEX"
	ConfGroupDataFormat = "DATA_FORMAT"
	ConfCompression     = "conf.dataFormatConfig.compression"
	None                = "NONE"
	Archive             = "ARCHIVE"
	Delete              = "DELETE"
)

type SpoolDirSource struct {
	*common.BaseStage
	Conf           SpoolDirConfigBean `ConfigDefBean:"conf"`
	spooler        *DirectorySpooler
	filePurger     *filePurger
	bufScanner     *bufio.Scanner
	file           *os.File
	cmpReader      *gzip.Reader
	csvHeaders     []*api.Field
	scannerAdvance int
	customDelim    string
}

type SpoolDirConfigBean struct {
	SpoolDir              string                            `ConfigDef:"type=STRING,required=true"`
	UseLastModified       string                            `ConfigDef:"type=STRING,required=true"`
	PoolingTimeoutSecs    float64                           `ConfigDef:"type=NUMBER,required=true"`
	SpoolingPeriod        float64                           `ConfigDef:"type=NUMBER,required=true"`
	InitialFileToProcess  string                            `ConfigDef:"type=STRING,required=true"`
	ProcessSubdirectories bool                              `ConfigDef:"type=BOOLEAN,required=true"`
	FilePattern           string                            `ConfigDef:"type=STRING,required=true"`
	PathMatcherMode       string                            `ConfigDef:"type=STRING,required=true"`
	ErrorArchiveDir       string                            `ConfigDef:"type=STRING,required=true"`
	PostProcessing        string                            `ConfigDef:"type=STRING,required=true"`
	ArchiveDir            string                            `ConfigDef:"type=STRING,required=true"`
	RetentionTimeMins     float64                           `ConfigDef:"type=NUMBER,required=true"`
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
		dirPath:                s.Conf.SpoolDir,
		readOrder:              s.Conf.UseLastModified,
		pathMatcherMode:        s.Conf.PathMatcherMode,
		filePattern:            s.Conf.FilePattern,
		processSubDirs:         s.Conf.ProcessSubdirectories,
		spoolingPeriodDuration: time.Duration(s.Conf.SpoolingPeriod) * time.Second,
		poolingTimeoutDuration: time.Duration(s.Conf.PoolingTimeoutSecs) * time.Second,
		stageContext:           stageContext,
	}

	if s.spooler.pathMatcherMode != Glob && s.spooler.pathMatcherMode != Regex {
		issues = append(issues, stageContext.CreateConfigIssue(
			"Unsupported Path Matcher mode :"+s.spooler.pathMatcherMode,
		))
		return issues
	}

	if s.Conf.DataFormatConfig.Compression != dataparser.CompressedNone &&
		s.Conf.DataFormatConfig.Compression != dataparser.CompressedFile {
		issues = append(issues, stageContext.CreateConfigIssue(
			"Unsupported Compression mode :"+s.Conf.DataFormatConfig.Compression,
			ConfGroupDataFormat,
			ConfCompression,
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

	if s.Conf.PostProcessing == Archive && s.Conf.RetentionTimeMins > 0 {
		s.filePurger = NewFilePurger(s.Conf)
		s.filePurger.run()
	}

	if s.Conf.DataFormatConfig.UseCustomDelimiter && len(s.Conf.DataFormatConfig.CustomDelimiter) > 0 {
		s.customDelim, _ = strconv.Unquote(`"` + s.Conf.DataFormatConfig.CustomDelimiter + `"`)

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

		if s.Conf.DataFormatConfig.Compression == dataparser.CompressedFile {
			s.cmpReader, err = gzip.NewReader(f)
			if err != nil {
				return err
			}
		}

		if s.Conf.DataFormat == "DELIMITED" && s.Conf.DataFormatConfig.CsvHeader == delimitedrecord.WithHeader &&
			(len(s.csvHeaders) == 0 || fInfo.getOffsetToRead() == 0) {
			s.initializeCSVHeaders(fInfo)
		}

		if err = s.seekAndInitializeBufferedReader(fInfo); err != nil {
			return err
		}

		if s.Conf.DataFormat == "DELIMITED" && fInfo.getOffsetToRead() == 0 {
			bytesRead := 0
			currentFileInfo := s.spooler.getCurrentFileInfo()
			if s.Conf.DataFormatConfig.CsvSkipStartLines > 0 {
				skippedLines := 0
				for skippedLines < int(s.Conf.DataFormatConfig.CsvSkipStartLines) {
					if ok := s.bufScanner.Scan(); ok {
						bytesRead += s.scannerAdvance
					}
					skippedLines++
				}
			} else if s.Conf.DataFormatConfig.CsvHeader == delimitedrecord.WithHeader ||
				s.Conf.DataFormatConfig.CsvHeader == delimitedrecord.IgnoreHeader {
				if ok := s.bufScanner.Scan(); ok {
					bytesRead += s.scannerAdvance
				}
			}
			currentFileInfo.incOffsetToRead(int64(bytesRead))
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

	// End of the file or empty offset, let's get a new file
	if currentFilePath == "" || currentStartOffset == EOFOffset {
		if currentFilePath != "" && s.Conf.PostProcessing != None {
			s.postProcessFile(currentFilePath)
		}
		nextFileInfoToProcess := s.spooler.NextFile()
		// No more files to process at the moment
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
	record, err := s.Conf.DataFormatConfig.RecordCreator.CreateRecord(
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
		if s.bufScanner == nil {
			// if pipeline stopped
			break
		}

		if ok := s.bufScanner.Scan(); !ok {
			err := s.bufScanner.Err()
			if err != nil && err != io.EOF {
				if len(s.Conf.ErrorArchiveDir) > 0 {
					s.handleErrorFile(s.spooler.getCurrentFileInfo())
				}
				log.WithError(s.bufScanner.Err()).Error("Error while reading file")
				s.GetStageContext().ReportError(s.bufScanner.Err())
				return startOffsetForBatch, nil
			}
			isEof = true
		}
		lineBytes := s.bufScanner.Bytes()
		bytesRead := len(lineBytes)
		if bytesRead > 0 {
			err := s.createRecordAndAddToBatch(
				recordReaderFactory,
				strings.TrimRight(string(lineBytes), "\r\n"),
				batchMaker,
			)

			if err != nil {
				if len(s.Conf.ErrorArchiveDir) > 0 {
					s.handleErrorFile(s.spooler.getCurrentFileInfo())
				}
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
		s.spooler.getCurrentFileInfo().incOffsetToRead(int64(s.scannerAdvance))
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

		if s.Conf.DataFormat == "WHOLE_FILE" {
			s.produceWholeFileRecord(batchMaker)
		} else {
			err = s.initializeBuffReaderIfNeeded()

			if err != nil {
				s.GetStageContext().ReportError(err)
				if len(s.Conf.ErrorArchiveDir) > 0 {
					s.handleErrorFile(s.spooler.getCurrentFileInfo())
				}
				return lastSourceOffset, nil
			}
			offset, err := s.readAndCreateRecords(maxBatchSize, batchMaker)

			if offset == InvalidOffset && err != nil {
				s.GetStageContext().ReportError(err)
				return lastSourceOffset, err
			}
		}

		newOffset := s.spooler.getCurrentFileInfo().createOffset()
		return &newOffset, nil
	}
	return lastSourceOffset, nil
}

func (s *SpoolDirSource) resetFileAndBuffReader() {
	if s.cmpReader != nil {
		// Close Quietly
		if err := s.cmpReader.Close(); err != nil {
			log.WithError(err).WithField("file", s.file.Name()).Error("Error During file close")
		}
		s.cmpReader = nil
	}
	if s.file != nil {
		// Close Quietly
		if err := s.file.Close(); err != nil {
			log.WithError(err).WithField("file", s.file.Name()).Error("Error During file close")
		}
		s.file = nil
	}
	s.bufScanner = nil
}

func (s *SpoolDirSource) seekAndInitializeBufferedReader(fInfo *AtomicFileInformation) error {
	s.scannerAdvance = 0
	if s.cmpReader != nil {
		// gzip has no Seek function because the file format simply does not allow it.
		// The only way to find byte N is to decompress and discard N bytes.
		s.bufScanner = bufio.NewScanner(s.cmpReader)
		s.bufScanner.Split(s.scannerSplitFunc)
		bytesDiscarded := int64(0)
		offsetRead := fInfo.getOffsetToRead()
		for bytesDiscarded < offsetRead {
			if ok := s.bufScanner.Scan(); !ok {
				log.WithError(s.bufScanner.Err()).Error("failed to seek")
				break
			}
			bytesDiscarded += int64(s.scannerAdvance)
		}
	} else {
		if _, err := s.file.Seek(fInfo.getOffsetToRead(), 0); err != nil {
			return err
		}
		s.bufScanner = bufio.NewScanner(s.file)
		s.bufScanner.Split(s.scannerSplitFunc)
	}
	return nil
}

func (s *SpoolDirSource) initializeCSVHeaders(fInfo *AtomicFileInformation) {
	var bufReader *bufio.Reader
	f, err := os.Open(fInfo.getFullPath())
	if err != nil {
		return
	}
	defer f.Close()
	if s.Conf.DataFormatConfig.Compression == dataparser.CompressedFile {
		gzipReader, err := gzip.NewReader(f)
		if err != nil {
			return
		}
		defer gzipReader.Close()
		bufReader = bufio.NewReader(gzipReader)
	} else {
		bufReader = bufio.NewReader(f)
	}

	headerLine, err := bufReader.ReadBytes('\n')
	if err == nil {
		columns := strings.Split(string(headerLine), ",")
		s.csvHeaders = make([]*api.Field, len(columns))
		for i, col := range columns {
			headerField, _ := api.CreateStringField(col)
			s.csvHeaders[i] = headerField
		}
	}
}

func (s *SpoolDirSource) scannerSplitFunc(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if len(s.customDelim) > 0 {
		// Return nothing if at end of file and no data passed
		if atEOF && len(data) == 0 {
			s.scannerAdvance = 0
			return 0, nil, nil
		}

		if i := strings.Index(string(data), s.customDelim); i >= 0 {
			s.scannerAdvance = i + 1
			return i + 1, data[0:i], nil
		}

		// If at end of file with data return the data
		if atEOF {
			s.scannerAdvance = len(data)
			return len(data), data, nil
		}
	} else {
		advance, token, err = bufio.ScanLines(data, atEOF)
		s.scannerAdvance = advance
	}
	return
}

func (s *SpoolDirSource) postProcessFile(fileFullPath string) {
	log.WithField("File Name", fileFullPath).
		WithField("option", s.Conf.PostProcessing).
		Debug("post processing file")
	if _, err := os.Stat(fileFullPath); !os.IsNotExist(err) {
		if s.Conf.PostProcessing == Archive {
			fileName := filepath.Base(fileFullPath)
			archiveFilePath := filepath.Join(s.Conf.ArchiveDir, fileName)
			err := os.Rename(fileFullPath, archiveFilePath)
			if err != nil {
				log.WithError(err).Error("failed to archive file")
				s.GetStageContext().ReportError(err)
			}
		} else if s.Conf.PostProcessing == Delete {
			err := os.Remove(fileFullPath)
			if err != nil {
				log.WithError(err).Error("failed to delete file")
				s.GetStageContext().ReportError(err)
			}
		}
	}
}

func (s *SpoolDirSource) handleErrorFile(fileInfo *AtomicFileInformation) {
	log.WithField("File Name", s.spooler.getCurrentFileInfo().getFullPath()).
		WithField("option", s.Conf.PostProcessing).
		Debug("error handling file")
	archiveFilePath := filepath.Join(s.Conf.ErrorArchiveDir, s.spooler.getCurrentFileInfo().getName())
	err := os.Rename(s.spooler.getCurrentFileInfo().getFullPath(), archiveFilePath)
	s.spooler.getCurrentFileInfo().setOffsetToRead(EOFOffset)
	s.resetFileAndBuffReader()
	if err != nil {
		log.WithError(err).Error("failed to archive error file")
		s.GetStageContext().ReportError(err)
	}
}

func (s *SpoolDirSource) Destroy() error {
	s.resetFileAndBuffReader()
	s.spooler.Destroy()
	if s.filePurger != nil {
		s.filePurger.destroy()
	}
	return nil
}

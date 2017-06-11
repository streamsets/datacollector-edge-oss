package spooler

import (
	"bufio"
	"errors"
	"github.com/streamsets/sdc2go/api"
	"github.com/streamsets/sdc2go/container/common"
	"github.com/streamsets/sdc2go/stages/stagelibrary"
	"io"
	"log"
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

	FILE      = "file"
	FILE_NAME = "filename"
	OFFSET    = "offset"
)

type SpoolDirSource struct {
	*common.BaseStage
	spooler              *DirectorySpooler
	bufReader            *bufio.Reader
	initialFileToProcess string
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
	stageConfig := s.GetStageConfig()
	s.spooler = &DirectorySpooler{}
	for _, config := range stageConfig.Configuration {
		value := s.GetStageContext().GetResolvedValue(config.Value)
		switch config.Name {
		case SPOOL_DIR_PATH:
			s.spooler.dirPath = value.(string)
		case USE_LAST_MODIFIED:
			s.spooler.readOrder = value.(string)
			readOrder = s.spooler.readOrder
		case FILE_PATTERN:
			//TODO: Handle regex
			s.spooler.filePattern = value.(string)
		case PROCESS_SUB_DIRECTORIES:
			s.spooler.processSubDirs = value.(bool)
		case POLLING_TIMEOUT_SECONDS:
			s.spooler.spoolWaitDuration = time.Duration(int64(value.(float64)) * 1000 * 1000)
		case INITIAL_FILE_TO_PROCESS:
			if value == nil {
				s.initialFileToProcess = ""
			} else {
				s.initialFileToProcess = value.(string)
			}
		}
	}
	s.spooler.Init()
	var err error = nil
	if s.initialFileToProcess != "" {
		file_matches, err := filepath.Glob(s.initialFileToProcess)
		if err == nil {
			if len(file_matches) > 1 {
				return errors.New(
					"Initial File to Process '" +
						s.initialFileToProcess + "' matches multiple files",
				)
			}
			s.initialFileToProcess = file_matches[0]
		}
	}
	return err
}

func (s *SpoolDirSource) initializeBuffReader() (*os.File, error) {
	fInfo := s.spooler.getCurrentFileInfo()
	f, err := os.Open(fInfo.getFullPath())
	if err != nil {
		return f, err
	}
	_, er := f.Seek(fInfo.getOffsetToRead(), 0)
	if er != nil {
		return f, er
	}
	s.bufReader = bufio.NewReader(f)
	return f, nil

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
	if currentFilePath == "" && s.initialFileToProcess != "" {
		fileInfo, err := os.Stat(currentFilePath)
		if err != nil {
			return false, err
		}
		s.spooler.setCurrentFileInfo(
			NewAtomicFileInformation(
				s.initialFileToProcess,
				fileInfo.ModTime(),
				currentStartOffset,
			),
		)
		log.Printf("[DEBUG] Using Initial File To Process '%s' ", currentFilePath)
	}

	//End of the file or empty offset, let's get a new file
	if currentFilePath == "" || currentStartOffset == -1 {
		nextFileInfoToProcess := s.spooler.NextFile()
		//No more files to process at the moment
		if nextFileInfoToProcess == nil {
			log.Println("[DEBUG] No more files to process")
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

		record := s.GetStageContext().CreateRecord(
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
				log.Printf("[Error] Error happened When reading file '%s'", err.Error())
				return startOffsetForBatch, err
			}
			isEof = true
		}
		bytesRead := len(line_bytes)

		s.createRecordAndAddToBatch(line_bytes, batchMaker)

		if isEof {
			log.Printf("[DEBUG] Reached End of File '%s'", s.spooler.getCurrentFileInfo().getFullPath())
			s.spooler.getCurrentFileInfo().setOffsetToRead(EOF_OFFSET)
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
		log.Printf("[ERROR] Error Happened : %s", err.Error())
		return lastSourceOffset, err
	}

	if shouldProduce {
		if s.spooler.getCurrentFileInfo() == nil {
			return lastSourceOffset, nil
		}

		//TODO Always seeks, we can do better
		f, err := s.initializeBuffReader()
		defer f.Close()

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

func (s *SpoolDirSource) Destroy() error {
	s.spooler.Destroy()
	return nil
}

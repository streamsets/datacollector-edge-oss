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
	"bytes"
	"compress/gzip"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
	"github.com/streamsets/datacollector-edge/container/recordio/delimitedrecord"
	"github.com/streamsets/datacollector-edge/stages/lib/dataparser"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const (
	SpoolDirPath          = "conf.spoolDir"
	UseLastModified       = "conf.useLastModified"
	SpoolingPeriod        = "conf.spoolingPeriod"
	PoolingTimeoutSecs    = "conf.poolingTimeoutSecs"
	InitialFileToProcess  = "conf.initialFileToProcess"
	ProcessSubdirectories = "conf.processSubdirectories"
	FilePattern           = "conf.filePattern"
	PathMatcherMode       = "conf.pathMatcherMode"
)

const sampleCsvData1 = `policyID,statecode,county,eq_site_limit,hu_site_limit,fl_site_limit,fr_site_limit,tiv_2011,tiv_2012,eq_site_deductible,hu_site_deductible,fl_site_deductible,fr_site_deductible,point_latitude,point_longitude,line,construction,point_granularity
119736,FL,CLAY COUNTY,498960,498960,498960,498960,498960,792148.9,0,9979.2,0,0,30.102261,-81.711777,Residential,Masonry,1
448094,FL,CLAY COUNTY,1322376.3,1322376.3,1322376.3,1322376.3,1322376.3,1438163.57,0,0,0,0,30.063936,-81.707664,Residential,Masonry,3
206893,FL,CLAY COUNTY,190724.4,190724.4,190724.4,190724.4,190724.4,192476.78,0,0,0,0,30.089579,-81.700455,Residential,Wood,1`

const sampleCsvData2 = `policyID,statecode,county,eq_site_limit,hu_site_limit,fl_site_limit,fr_site_limit,tiv_2011,tiv_2012,eq_site_deductible,hu_site_deductible,fl_site_deductible,fr_site_deductible,point_latitude,point_longitude,line,construction,point_granularity
119736,FL,CLAY COUNTY,498960,498960,498960,498960,498960,792148.9,0,9979.2,0,0,30.102261,-81.711777,Residential,Masonry,1
448094,FL,CLAY COUNTY,1322376.3,1322376.3,1322376.3,1322376.3,1322376.3,1438163.57,0,0,0,0,30.063936,-81.707664,Residential,Masonry,3`

const sampleCsvData3 = `policyID,statecode,county,eq_site_limit,hu_site_limit,fl_site_limit,fr_site_limit,tiv_2011,tiv_2012,eq_site_deductible,hu_site_deductible,fl_site_deductible,fr_site_deductible,point_latitude,point_longitude,line,construction,point_granularity
119736,FL,CLAY COUNTY,498960,498960,498960,498960,498960,792148.9,0,9979.2,0,0,30.102261,-81.711777,Residential,Masonry,1`

func createStageContext(config []common.Config) *common.StageContextImpl {
	stageConfig := common.StageConfiguration{}
	stageConfig.Library = Library
	stageConfig.StageName = StageName
	stageConfig.Configuration = config
	errorSink := common.NewErrorSink()
	return &common.StageContextImpl{
		StageConfig: &stageConfig,
		Parameters:  nil,
		ErrorSink:   errorSink,
	}
}

func getStageConfig(
	dirPath string,
	processSubDirectories bool,
	pathMatherMode string,
	filePattern string,
	useLastModified bool,
	initialFileToProcess string,
	spoolingPeriod int64,
	dataFormat string,
	compressionType string,
) []common.Config {
	configuration := make([]common.Config, 11)

	configuration[0] = common.Config{
		Name:  SpoolDirPath,
		Value: dirPath,
	}

	configuration[1] = common.Config{
		Name:  ProcessSubdirectories,
		Value: processSubDirectories,
	}

	configuration[2] = common.Config{
		Name:  PathMatcherMode,
		Value: pathMatherMode,
	}

	configuration[3] = common.Config{
		Name:  FilePattern,
		Value: filePattern,
	}

	readOrder := Lexicographical

	if useLastModified {
		readOrder = Timestamp
	}

	configuration[4] = common.Config{
		Name:  UseLastModified,
		Value: readOrder,
	}

	configuration[5] = common.Config{
		Name:  InitialFileToProcess,
		Value: initialFileToProcess,
	}

	configuration[6] = common.Config{
		Name:  SpoolingPeriod,
		Value: float64(spoolingPeriod),
	}

	configuration[7] = common.Config{
		Name:  "conf.dataFormat",
		Value: dataFormat,
	}

	configuration[8] = common.Config{
		Name:  ConfCompression,
		Value: compressionType,
	}

	configuration[9] = common.Config{
		Name:  PoolingTimeoutSecs,
		Value: float64(1),
	}

	configuration[10] = common.Config{
		Name:  "conf.dataFormatConfig.textMaxLineLen",
		Value: float64(1024),
	}

	return configuration
}

func createTestDirectory(t *testing.T) string {
	testDir, err := ioutil.TempDir("", t.Name())
	if err != nil {
		t.Fatalf("Error happened when creating test Directory, Reason : %s", err.Error())
	}
	t.Logf("Created Test Directory : '%s'", testDir)
	return testDir
}

func deleteTestDirectory(t *testing.T, testDir string) {
	t.Logf("Deleting Test Directory : '%s'", testDir)
	err := os.RemoveAll(testDir)
	if err != nil {
		t.Fatalf(
			"Error happened when deleting test Directory '%s', Reason: %s",
			testDir, err.Error())
	}
}

func createFileAndWriteContents(t *testing.T, filePath string, data string, compression string) {
	if compression == dataparser.CompressedFile {
		createCompressionFiles(t, filePath, data)
	} else {
		createNonCompressionFiles(t, filePath, data)
	}
}

func createCompressionFiles(t *testing.T, filePath string, data string) {
	f, err := os.Create(filePath)
	defer f.Close()
	w := gzip.NewWriter(f)
	defer w.Close()
	if err != nil {
		t.Fatalf("Error Creating file '%s'", filePath)
	}
	t.Logf("Successfully created File : %s", filePath)
	_, err = w.Write([]byte(data))
	if err != nil {
		t.Fatalf("Error Writing to file '%s'", filePath)
	}
	_ = f.Sync()
}

func createNonCompressionFiles(t *testing.T, filePath string, data string) {
	f, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("Error Creating file '%s'", filePath)
	}
	t.Logf("Successfully created File : %s", filePath)
	defer f.Sync()
	defer f.Close()
	_, err = f.WriteString(data)
	if err != nil {
		t.Fatalf("Error Writing to file '%s'", filePath)
	}
}

func createSpooler(t *testing.T, stageContext *common.StageContextImpl) api.Stage {
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage
	issues := stageInstance.Init(stageContext)
	if len(issues) != 0 {
		t.Error(issues[0].Message)
	}
	return stageInstance
}

func createSpoolerAndRun(
	t *testing.T,
	stageContext *common.StageContextImpl,
	lastSourceOffset string,
	batchSize int,
) (string, []api.Record) {
	stageInstance := createSpooler(t, stageContext)
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)

	offset, err := stageInstance.(api.Origin).Produce(&lastSourceOffset, batchSize, batchMaker)
	if err != nil {
		t.Fatal("Err :", err)
	}

	_ = stageInstance.Destroy()

	return *offset, batchMaker.GetStageOutput()
}

func checkRecord(
	t *testing.T,
	record api.Record,
	fieldPath string,
	value interface{},
	headersToCheck map[string]string,
) {
	isError := false
	expectedValue := value.(string)

	rootField, _ := record.Get(fieldPath)
	actualValue := rootField.Value.(string)
	actualHeaders := record.GetHeader().GetAttributes()

	if actualValue != expectedValue {
		isError = true
		t.Errorf(
			"Record value does not match, Expected : '%s', Actual : '%s'",
			expectedValue,
			actualValue,
		)
	}
	for headerName, expectedHeaderVal := range headersToCheck {
		actualHeaderVal := actualHeaders[headerName]
		if actualHeaderVal != expectedHeaderVal {
			isError = true
			t.Errorf(
				"Record Header '%s' does not match, Expected : '%s', Actual : '%s'",
				headerName,
				expectedHeaderVal,
				actualHeaderVal,
			)
		}
	}

	if isError {
		t.Fatalf(
			"Error happened when asserting record values/headers :'%s'",
			record.GetHeader().GetSourceId(),
		)
	}
}

func TestSpoolDirSource_Init_InvalidDataFormat(t *testing.T) {
	testDir := createTestDirectory(t)
	stageConfig := getStageConfig(testDir, false, Regex, "(.*)[.]txt", true, "", 1, "LOG", dataparser.CompressedNone)
	stageContext := createStageContext(stageConfig)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage
	issues := stageInstance.Init(stageContext)
	if len(issues) != 1 {
		t.Error("Expected Unsupported Data Format - LOG error")
	}
}

func TestUseLastModified(t *testing.T) {
	testUseLastModified(t, dataparser.CompressedNone)
	testUseLastModified(t, dataparser.CompressedFile)
}

func testUseLastModified(t *testing.T, compression string) {
	testDir := createTestDirectory(t)

	defer deleteTestDirectory(t, testDir)

	//Create a.txt,c.txt,b.txt with different mod times
	createFileAndWriteContents(t, filepath.Join(testDir, "a.txt"), "123\r\n456", compression)
	createFileAndWriteContents(t, filepath.Join(testDir, "c.txt"), "111112113\n114115116\n117118119", compression)
	createFileAndWriteContents(t, filepath.Join(testDir, "b.txt"), "111213\n141516", compression)

	currentTime := time.Now()

	_ = os.Chtimes(
		filepath.Join(testDir, "a.txt"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(3*time.Second).Nanoseconds()))
	_ = os.Chtimes(
		filepath.Join(testDir, "c.txt"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(2*time.Second).Nanoseconds()))
	_ = os.Chtimes(
		filepath.Join(testDir, "b.txt"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(time.Second).Nanoseconds()))

	stageConfig := getStageConfig(testDir, false, Regex, "(.*)[.]txt", true, "", 1, "TEXT", compression)
	stageContext := createStageContext(stageConfig)

	offset, records := createSpoolerAndRun(t, stageContext, "", 3)

	if len(records) != 2 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 2)
	}

	expectedHeaders := map[string]string{
		File:     filepath.Join(testDir, "a.txt"),
		FileName: "a.txt",
		Offset:   "0",
	}

	checkRecord(t, records[0], "/text", "123", expectedHeaders)

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "a.txt"),
		FileName: "a.txt",
		Offset:   "5",
	}

	checkRecord(t, records[1], "/text", "456", expectedHeaders)

	offset, records = createSpoolerAndRun(t, stageContext, offset, 2)

	if len(records) != 2 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 2)
	}

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "c.txt"),
		FileName: "c.txt",
		Offset:   "0",
	}

	checkRecord(t, records[0], "/text", "111112113", expectedHeaders)

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "c.txt"),
		FileName: "c.txt",
		Offset:   "10",
	}

	checkRecord(t, records[1], "/text", "114115116", expectedHeaders)

	offset, records = createSpoolerAndRun(t, stageContext, offset, 2)

	if len(records) != 1 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 1)
	}

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "c.txt"),
		FileName: "c.txt",
		Offset:   "20",
	}

	checkRecord(t, records[0], "/text", "117118119", expectedHeaders)

	offset, records = createSpoolerAndRun(t, stageContext, offset, 2)

	if len(records) != 2 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 2)
	}

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "b.txt"),
		FileName: "b.txt",
		Offset:   "0",
	}

	checkRecord(t, records[0], "/text", "111213", expectedHeaders)

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "b.txt"),
		FileName: "b.txt",
		Offset:   "7",
	}

	checkRecord(t, records[1], "/text", "141516", expectedHeaders)
}

func TestLexicographical(t *testing.T) {
	testLexicographical(t, dataparser.CompressedNone)
	testLexicographical(t, dataparser.CompressedFile)
}

func testLexicographical(t *testing.T, compression string) {

	testDir := createTestDirectory(t)

	defer deleteTestDirectory(t, testDir)

	//Create a.txt,c.txt,b.txt with different mod times
	createFileAndWriteContents(t, filepath.Join(testDir, "a.txt"), "123\n456", compression)
	createFileAndWriteContents(t, filepath.Join(testDir, "b.txt"), "111213\n141516", compression)
	createFileAndWriteContents(t, filepath.Join(testDir, "c.txt"), "111112113\n114115116\n117118119", compression)

	currentTime := time.Now()

	_ = os.Chtimes(
		filepath.Join(testDir, "a.txt"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(3*time.Second).Nanoseconds()))
	_ = os.Chtimes(
		filepath.Join(testDir, "b.txt"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(2*time.Second).Nanoseconds()))
	_ = os.Chtimes(
		filepath.Join(testDir, "c.txt"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(time.Second).Nanoseconds()))

	stageConfig := getStageConfig(testDir, false, Glob, "*", false, "", 1, "TEXT", compression)
	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.postProcessing",
		Value: Delete,
	})
	stageContext := createStageContext(stageConfig)

	offset, records := createSpoolerAndRun(t, stageContext, "", 3)

	if len(records) != 2 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 2)
	}

	expectedHeaders := map[string]string{
		File:     filepath.Join(testDir, "a.txt"),
		FileName: "a.txt",
		Offset:   "0",
	}

	checkRecord(t, records[0], "/text", "123", expectedHeaders)

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "a.txt"),
		FileName: "a.txt",
		Offset:   "4",
	}

	checkRecord(t, records[1], "/text", "456", expectedHeaders)

	offset, records = createSpoolerAndRun(t, stageContext, offset, 3)

	if len(records) != 2 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 2)
	}

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "b.txt"),
		FileName: "b.txt",
		Offset:   "0",
	}

	checkRecord(t, records[0], "/text", "111213", expectedHeaders)

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "b.txt"),
		FileName: "b.txt",
		Offset:   "7",
	}

	checkRecord(t, records[1], "/text", "141516", expectedHeaders)

	offset, records = createSpoolerAndRun(t, stageContext, offset, 2)

	if len(records) != 2 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 2)
	}

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "c.txt"),
		FileName: "c.txt",
		Offset:   "0",
	}

	checkRecord(t, records[0], "/text", "111112113", expectedHeaders)

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "c.txt"),
		FileName: "c.txt",
		Offset:   "10",
	}

	checkRecord(t, records[1], "/text", "114115116", expectedHeaders)

	offset, records = createSpoolerAndRun(t, stageContext, offset, 2)

	if len(records) != 1 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 1)
	}

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "c.txt"),
		FileName: "c.txt",
		Offset:   "20",
	}

	checkRecord(t, records[0], "/text", "117118119", expectedHeaders)

	// call another batch for post processing
	offset, records = createSpoolerAndRun(t, stageContext, offset, 2)

	// validate post processing
	files, _ := ioutil.ReadDir(testDir)
	if len(files) != 0 {
		t.Error("Failed to post process files - delete option")
	}
}

func TestSubDirectories(t *testing.T) {
	testSubDirectories(t, dataparser.CompressedNone)
	testSubDirectories(t, dataparser.CompressedFile)
}

func testSubDirectories(t *testing.T, compression string) {
	testDir := createTestDirectory(t)
	defer deleteTestDirectory(t, testDir)

	allLetters := []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

	pathsToCreate := []string{
		"a/b",
		"b/c/d",
		"e/f/g/h",
		"i/j",
		"k/l/m/n",
		"o/p/q/r/s",
		"u",
		"v/w",
		"x/y/z",
	}

	var createdFiles []string

	currentTime := time.Now()

	for _, pathToCreate := range pathsToCreate {
		pathToCreate = filepath.Join(testDir, pathToCreate)
		err := os.MkdirAll(pathToCreate, 0777)
		if err != nil {
			t.Fatalf("Error when creating folder: '%s'", pathToCreate)
		}
		fileToCreate := filepath.Join(
			pathToCreate,
			string(allLetters[rand.Intn(len(allLetters)-1)]))
		createFileAndWriteContents(t, fileToCreate, "sample text", compression)
		_ = os.Chtimes(
			fileToCreate, currentTime,
			time.Unix(0, currentTime.UnixNano()+
				(int64(len(createdFiles))*time.Second.Nanoseconds())))
		createdFiles = append(createdFiles, fileToCreate)
	}

	stageConfig := getStageConfig(testDir, true, Glob, "*", true, "", 1, "TEXT", compression)
	stageContext := createStageContext(stageConfig)

	var offset = ""
	var records []api.Record

	for _, fileToCreate := range createdFiles {
		offset, records = createSpoolerAndRun(t, stageContext, offset, 10)

		if len(records) != 1 {
			t.Fatalf(
				"Wrong number of records, Actual : %d, Expected : %d ",
				len(records),
				1,
			)
		}

		expectedHeaders := map[string]string{
			File:     fileToCreate,
			FileName: filepath.Base(fileToCreate),
			Offset:   "0",
		}

		checkRecord(t, records[0], "/text", "sample text", expectedHeaders)
	}
}

func TestReadingFileAcrossBatches(t *testing.T) {
	testReadingFileAcrossBatches(t, dataparser.CompressedNone)
	testReadingFileAcrossBatches(t, dataparser.CompressedFile)
}

func testReadingFileAcrossBatches(t *testing.T, compression string) {
	testDir := createTestDirectory(t)
	defer deleteTestDirectory(t, testDir)

	allLetters := []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

	var expectedRecordContents []string
	contents := bytes.NewBuffer([]byte{})

	totalLines, totalCharactersInLine := 100, 20

	for line := 0; line < totalLines; line++ {
		var currentLine = ""
		for lineLetters := 0; lineLetters < totalCharactersInLine; lineLetters++ {
			currentLine = currentLine + string(allLetters[rand.Intn(len(allLetters)-1)])

		}
		expectedRecordContents = append(expectedRecordContents, currentLine)
		contents.WriteString(currentLine + "\n")
	}

	//Create a.txt,c.txt,b.txt with different mod times
	createFileAndWriteContents(t, filepath.Join(testDir, "a.txt"), contents.String(), compression)
	stageConfig := getStageConfig(testDir, false, Regex, ".*", true, "", 1, "TEXT", compression)
	stageInstance := createSpooler(t, createStageContext(stageConfig))
	defer stageInstance.Destroy()

	noOfRecords := 0

	lastSourceOffsetStr := ""
	lastSourceOffset := &lastSourceOffsetStr

	for noOfRecords < totalLines {
		batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
		lastSourceOffset, _ = stageInstance.(api.Origin).Produce(lastSourceOffset, rand.Intn(19)+1, batchMaker)
		records := batchMaker.GetStageOutput()
		for rIdx, record := range records {
			checkRecord(t, record, "/text", expectedRecordContents[noOfRecords+rIdx], map[string]string{})
		}
		noOfRecords += len(records)
	}

	//No more records to read
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	lastSourceOffset, _ = stageInstance.(api.Origin).Produce(lastSourceOffset, rand.Intn(19)+1, batchMaker)
	if len(batchMaker.GetStageOutput()) != 0 {
		t.Fatal("Read more number of records than expected")
	}
}

func TestLexicographical_JSON_FORMAT(t *testing.T) {
	testLexicographical_JSON_FORMAT(t, dataparser.CompressedNone)
	testLexicographical_JSON_FORMAT(t, dataparser.CompressedFile)
}

func testLexicographical_JSON_FORMAT(t *testing.T, compression string) {

	testDir := createTestDirectory(t)
	archiveDir := createTestDirectory(t)

	defer deleteTestDirectory(t, testDir)

	//Create a.txt,c.txt,b.txt with different mod times
	createFileAndWriteContents(t, filepath.Join(testDir, "a.txt"), "{\"text\": \"123\"}\n{\"text\": \"456\"}", compression)
	createFileAndWriteContents(t, filepath.Join(testDir, "b.txt"), "{\"text\": \"111213\"}\n{\"text\": \"141516\"}", compression)
	createFileAndWriteContents(t, filepath.Join(testDir, "c.txt"), "{\"text\": \"111112113\"}\n{\"text\": \"117118119\"}", compression)

	currentTime := time.Now()

	_ = os.Chtimes(
		filepath.Join(testDir, "a.txt"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(3*time.Second).Nanoseconds()))
	_ = os.Chtimes(
		filepath.Join(testDir, "b.txt"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(2*time.Second).Nanoseconds()))
	_ = os.Chtimes(
		filepath.Join(testDir, "c.txt"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(time.Second).Nanoseconds()))
	stageConfig := getStageConfig(testDir, false, Glob, "*", false, "", 1, "JSON", compression)
	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.postProcessing",
		Value: Archive,
	})
	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.archiveDir",
		Value: archiveDir,
	})
	stageContext := createStageContext(stageConfig)

	offset, records := createSpoolerAndRun(t, stageContext, "", 3)

	if len(records) != 2 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 2)
	}

	expectedHeaders := map[string]string{
		File:     filepath.Join(testDir, "a.txt"),
		FileName: "a.txt",
		Offset:   "0",
	}

	checkRecord(t, records[0], "/text", "123", expectedHeaders)

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "a.txt"),
		FileName: "a.txt",
		Offset:   "16",
	}

	checkRecord(t, records[1], "/text", "456", expectedHeaders)

	offset, records = createSpoolerAndRun(t, stageContext, offset, 3)

	if len(records) != 2 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 2)
	}

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "b.txt"),
		FileName: "b.txt",
		Offset:   "0",
	}

	checkRecord(t, records[0], "/text", "111213", expectedHeaders)

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "b.txt"),
		FileName: "b.txt",
		Offset:   "19",
	}

	checkRecord(t, records[1], "/text", "141516", expectedHeaders)

	offset, records = createSpoolerAndRun(t, stageContext, offset, 3)

	if len(records) != 2 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 2)
	}

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "c.txt"),
		FileName: "c.txt",
		Offset:   "0",
	}

	checkRecord(t, records[0], "/text", "111112113", expectedHeaders)

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "c.txt"),
		FileName: "c.txt",
		Offset:   "22",
	}

	checkRecord(t, records[1], "/text", "117118119", expectedHeaders)

	// call another batch for post processing
	offset, records = createSpoolerAndRun(t, stageContext, offset, 2)

	// validate post processing
	files, _ := ioutil.ReadDir(testDir)
	if len(files) != 0 {
		t.Error("Failed to post process files - archive option")
	}

	archivedFiles, _ := ioutil.ReadDir(archiveDir)
	if len(archivedFiles) != 3 {
		t.Error("Failed to post process files - archive option")
	}
}

func TestLexicographical_DELIMITED_FORMAT_NO_HEADER(t *testing.T) {
	testLexicographical_DELIMITED_FORMAT_NO_HEADER(t, dataparser.CompressedNone)
	testLexicographical_DELIMITED_FORMAT_NO_HEADER(t, dataparser.CompressedFile)
}

func testLexicographical_DELIMITED_FORMAT_NO_HEADER(t *testing.T, compression string) {

	testDir := createTestDirectory(t)

	defer deleteTestDirectory(t, testDir)

	// Create a.csv,b.csv,c.csv with different mod times
	createFileAndWriteContents(t, filepath.Join(testDir, "a.csv"), sampleCsvData1, compression)
	createFileAndWriteContents(t, filepath.Join(testDir, "b.csv"), sampleCsvData2, compression)
	createFileAndWriteContents(t, filepath.Join(testDir, "c.csv"), sampleCsvData3, compression)

	currentTime := time.Now()

	_ = os.Chtimes(
		filepath.Join(testDir, "a.csv"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(3*time.Second).Nanoseconds()))
	_ = os.Chtimes(
		filepath.Join(testDir, "b.csv"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(2*time.Second).Nanoseconds()))
	_ = os.Chtimes(
		filepath.Join(testDir, "c.csv"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(time.Second).Nanoseconds()))

	stageConfig := getStageConfig(testDir, false, Glob, "*", false, "", 1, "DELIMITED", compression)
	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.dataFormatConfig.csvRecordType",
		Value: delimitedrecord.ListMap,
	})
	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.dataFormatConfig.csvHeader",
		Value: delimitedrecord.NoHeader,
	})

	stageContext := createStageContext(stageConfig)

	offset, records := createSpoolerAndRun(t, stageContext, "", 3)

	if len(records) != 3 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 3)
	}

	expectedHeaders := map[string]string{
		File:     filepath.Join(testDir, "a.csv"),
		FileName: "a.csv",
		Offset:   "0",
	}

	checkRecord(t, records[0], "/0", "policyID", expectedHeaders)

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "a.csv"),
		FileName: "a.csv",
		Offset:   "243",
	}

	checkRecord(t, records[1], "/0", "119736", expectedHeaders)

	offset, records = createSpoolerAndRun(t, stageContext, offset, 2)

	if len(records) != 1 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 1)
	}

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "a.csv"),
		FileName: "a.csv",
		Offset:   "499",
	}

	checkRecord(t, records[0], "/0", "206893", expectedHeaders)

	offset, records = createSpoolerAndRun(t, stageContext, offset, 4)

	if len(records) != 3 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 3)
	}

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "b.csv"),
		FileName: "b.csv",
		Offset:   "0",
	}

	checkRecord(t, records[0], "/0", "policyID", expectedHeaders)

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "b.csv"),
		FileName: "b.csv",
		Offset:   "243",
	}

	checkRecord(t, records[1], "/0", "119736", expectedHeaders)

	offset, records = createSpoolerAndRun(t, stageContext, offset, 20)

	if len(records) != 2 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 2)
	}

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "c.csv"),
		FileName: "c.csv",
		Offset:   "0",
	}

	checkRecord(t, records[0], "/0", "policyID", expectedHeaders)

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "c.csv"),
		FileName: "c.csv",
		Offset:   "243",
	}

	checkRecord(t, records[1], "/0", "119736", expectedHeaders)
}

func TestLexicographical_DELIMITED_FORMAT_WITH_HEADER(t *testing.T) {
	testLexicographical_DELIMITED_FORMAT_WITH_HEADER(t, dataparser.CompressedNone)
	testLexicographical_DELIMITED_FORMAT_WITH_HEADER(t, dataparser.CompressedFile)
}

func testLexicographical_DELIMITED_FORMAT_WITH_HEADER(t *testing.T, compression string) {

	testDir := createTestDirectory(t)

	defer deleteTestDirectory(t, testDir)

	// Create a.csv,b.csv,c.csv with different mod times
	createFileAndWriteContents(t, filepath.Join(testDir, "a.csv"), sampleCsvData1, compression)
	createFileAndWriteContents(t, filepath.Join(testDir, "b.csv"), sampleCsvData2, compression)
	createFileAndWriteContents(t, filepath.Join(testDir, "c.csv"), sampleCsvData3, compression)

	currentTime := time.Now()

	_ = os.Chtimes(
		filepath.Join(testDir, "a.csv"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(3*time.Second).Nanoseconds()))
	_ = os.Chtimes(
		filepath.Join(testDir, "b.csv"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(2*time.Second).Nanoseconds()))
	_ = os.Chtimes(
		filepath.Join(testDir, "c.csv"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(time.Second).Nanoseconds()))

	stageConfig := getStageConfig(testDir, false, Glob, "*", false, "", 1, "DELIMITED", compression)
	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.dataFormatConfig.csvRecordType",
		Value: delimitedrecord.ListMap,
	})
	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.dataFormatConfig.csvHeader",
		Value: delimitedrecord.WithHeader,
	})

	stageContext := createStageContext(stageConfig)

	// read first header line + 2 lines from a.csv
	offset, records := createSpoolerAndRun(t, stageContext, "", 2)

	if len(records) != 2 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 2)
	}

	expectedHeaders := map[string]string{
		File:     filepath.Join(testDir, "a.csv"),
		FileName: "a.csv",
		Offset:   "243",
	}

	checkRecord(t, records[0], "/policyID", "119736", expectedHeaders)

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "a.csv"),
		FileName: "a.csv",
		Offset:   "365",
	}

	checkRecord(t, records[1], "/statecode", "FL", expectedHeaders)

	// read first last(4th) line from a.csv
	offset, records = createSpoolerAndRun(t, stageContext, offset, 2)

	if len(records) != 1 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 1)
	}

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "a.csv"),
		FileName: "a.csv",
		Offset:   "499",
	}

	checkRecord(t, records[0], "/policyID", "206893", expectedHeaders)

	// read 3 lines + header from b.csv
	offset, records = createSpoolerAndRun(t, stageContext, offset, 3)

	if len(records) != 2 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 3)
	}

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "b.csv"),
		FileName: "b.csv",
		Offset:   "243",
	}

	checkRecord(t, records[0], "/policyID", "119736", expectedHeaders)

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "b.csv"),
		FileName: "b.csv",
		Offset:   "365",
	}

	checkRecord(t, records[1], "/statecode", "FL", expectedHeaders)

	// read 1 line (with header) from c.csv
	offset, records = createSpoolerAndRun(t, stageContext, offset, 20)

	if len(records) != 1 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 1)
	}

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "c.csv"),
		FileName: "c.csv",
		Offset:   "243",
	}

	checkRecord(t, records[0], "/policyID", "119736", expectedHeaders)

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "c.csv"),
		FileName: "c.csv",
		Offset:   "243",
	}
}

func TestLexicographical_DELIMITED_FORMAT_IGNORE_HEADER(t *testing.T) {
	testLexicographical_DELIMITED_FORMAT_IGNORE_HEADER(t, dataparser.CompressedNone)
	testLexicographical_DELIMITED_FORMAT_IGNORE_HEADER(t, dataparser.CompressedFile)
}

func testLexicographical_DELIMITED_FORMAT_IGNORE_HEADER(t *testing.T, compression string) {

	testDir := createTestDirectory(t)

	defer deleteTestDirectory(t, testDir)

	// Create a.csv,b.csv,c.csv with different mod times
	createFileAndWriteContents(t, filepath.Join(testDir, "a.csv"), sampleCsvData1, compression)
	createFileAndWriteContents(t, filepath.Join(testDir, "b.csv"), sampleCsvData2, compression)
	createFileAndWriteContents(t, filepath.Join(testDir, "c.csv"), sampleCsvData3, compression)

	currentTime := time.Now()

	_ = os.Chtimes(
		filepath.Join(testDir, "a.csv"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(3*time.Second).Nanoseconds()))
	_ = os.Chtimes(
		filepath.Join(testDir, "b.csv"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(2*time.Second).Nanoseconds()))
	_ = os.Chtimes(
		filepath.Join(testDir, "c.csv"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(time.Second).Nanoseconds()))

	stageConfig := getStageConfig(testDir, false, Glob, "*", false, "", 1, "DELIMITED", compression)
	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.dataFormatConfig.csvRecordType",
		Value: delimitedrecord.ListMap,
	})
	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.dataFormatConfig.csvHeader",
		Value: delimitedrecord.IgnoreHeader,
	})

	stageContext := createStageContext(stageConfig)

	// read first header line + 2 lines from a.csv
	offset, records := createSpoolerAndRun(t, stageContext, "", 2)

	if len(records) != 2 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 2)
	}

	expectedHeaders := map[string]string{
		File:     filepath.Join(testDir, "a.csv"),
		FileName: "a.csv",
		Offset:   "243",
	}

	checkRecord(t, records[0], "/0", "119736", expectedHeaders)

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "a.csv"),
		FileName: "a.csv",
		Offset:   "365",
	}

	checkRecord(t, records[1], "/1", "FL", expectedHeaders)

	// read first last(4th) line from a.csv
	offset, records = createSpoolerAndRun(t, stageContext, offset, 2)

	if len(records) != 1 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 1)
	}

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "a.csv"),
		FileName: "a.csv",
		Offset:   "499",
	}

	checkRecord(t, records[0], "/0", "206893", expectedHeaders)

	// read 3 lines + header from b.csv
	offset, records = createSpoolerAndRun(t, stageContext, offset, 3)

	if len(records) != 2 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 3)
	}

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "b.csv"),
		FileName: "b.csv",
		Offset:   "243",
	}

	checkRecord(t, records[0], "/0", "119736", expectedHeaders)

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "b.csv"),
		FileName: "b.csv",
		Offset:   "365",
	}

	checkRecord(t, records[1], "/1", "FL", expectedHeaders)

	// read 1 line (with header) from c.csv
	offset, records = createSpoolerAndRun(t, stageContext, offset, 20)

	if len(records) != 1 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 1)
	}

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "c.csv"),
		FileName: "c.csv",
		Offset:   "243",
	}

	checkRecord(t, records[0], "/0", "119736", expectedHeaders)

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "c.csv"),
		FileName: "c.csv",
		Offset:   "243",
	}
}

func TestLexicographical_DELIMITED_FORMAT_SKIP_START_LINES(t *testing.T) {
	testLexicographical_DELIMITED_FORMAT_SKIP_START_LINES(t, dataparser.CompressedNone)
	testLexicographical_DELIMITED_FORMAT_SKIP_START_LINES(t, dataparser.CompressedFile)
}

func testLexicographical_DELIMITED_FORMAT_SKIP_START_LINES(t *testing.T, compression string) {

	testDir := createTestDirectory(t)

	defer deleteTestDirectory(t, testDir)

	// Create a.csv,b.csv,c.csv with different mod times
	createFileAndWriteContents(t, filepath.Join(testDir, "a.csv"), sampleCsvData1, compression)
	createFileAndWriteContents(t, filepath.Join(testDir, "b.csv"), sampleCsvData2, compression)
	createFileAndWriteContents(t, filepath.Join(testDir, "c.csv"), sampleCsvData3, compression)

	currentTime := time.Now()

	_ = os.Chtimes(
		filepath.Join(testDir, "a.csv"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(3*time.Second).Nanoseconds()))
	_ = os.Chtimes(
		filepath.Join(testDir, "b.csv"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(2*time.Second).Nanoseconds()))
	_ = os.Chtimes(
		filepath.Join(testDir, "c.csv"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(time.Second).Nanoseconds()))

	stageConfig := getStageConfig(testDir, false, Glob, "*", false, "", 1, "DELIMITED", compression)
	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.dataFormatConfig.csvRecordType",
		Value: delimitedrecord.ListMap,
	})
	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.dataFormatConfig.csvHeader",
		Value: delimitedrecord.IgnoreHeader,
	})
	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.dataFormatConfig.csvSkipStartLines",
		Value: float64(2),
	})

	stageContext := createStageContext(stageConfig)

	// skip 2 lines and read 3rd from a.csv
	offset, records := createSpoolerAndRun(t, stageContext, "", 1)

	if len(records) != 1 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 1)
	}

	expectedHeaders := map[string]string{
		File:     filepath.Join(testDir, "a.csv"),
		FileName: "a.csv",
		Offset:   "365",
	}

	checkRecord(t, records[0], "/0", "448094", expectedHeaders)

	// read last(4th) line from a.csv
	offset, records = createSpoolerAndRun(t, stageContext, offset, 2)

	if len(records) != 1 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 1)
	}

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "a.csv"),
		FileName: "a.csv",
		Offset:   "499",
	}

	checkRecord(t, records[0], "/0", "206893", expectedHeaders)

	// skip 2 lines and read 3rd line from b.csv
	offset, records = createSpoolerAndRun(t, stageContext, offset, 3)

	if len(records) != 1 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 3)
	}

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "b.csv"),
		FileName: "b.csv",
		Offset:   "365",
	}

	checkRecord(t, records[0], "/0", "448094", expectedHeaders)
}

func TestCustomDelimiter1(t *testing.T) {
	testCustomDelimiter1(t, dataparser.CompressedNone)
	testCustomDelimiter1(t, dataparser.CompressedFile)
}

func testCustomDelimiter1(t *testing.T, compression string) {
	testDir := createTestDirectory(t)

	defer deleteTestDirectory(t, testDir)

	testData := `a: This is attribute 1 record of record 1
b: This is attribute 2 record of record 1
c: This is attribute 3 record of record 1
d: This is attribute 4 record of record 1
e: This is attribute 5 record of record 1

a: This is attribute 1 record of record 2
b: This is attribute 2 record of record 2
c: This is attribute 3 record of record 2
d: This is attribute 4 record of record 2
e: This is attribute 5 record of record 2

a: This is attribute 1 record of record 3
b: This is attribute 2 record of record 3
c: This is attribute 3 record of record 3
d: This is attribute 4 record of record 3
e: This is attribute 5 record of record 3
`

	createFileAndWriteContents(t, filepath.Join(testDir, "a.txt"), testData, compression)
	currentTime := time.Now()

	_ = os.Chtimes(
		filepath.Join(testDir, "a.txt"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(3*time.Second).Nanoseconds()))

	stageConfig := getStageConfig(testDir, false, Regex, "(.*)[.]txt", true, "", 1, "TEXT", compression)

	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.dataFormatConfig.useCustomDelimiter",
		Value: "true",
	})
	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.dataFormatConfig.customDelimiter",
		Value: "\\n\\n",
	})

	stageContext := createStageContext(stageConfig)

	_, records := createSpoolerAndRun(t, stageContext, "", 3)

	if len(records) != 3 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 2)
	}

	expectedHeaders := map[string]string{
		File:     filepath.Join(testDir, "a.txt"),
		FileName: "a.txt",
		Offset:   "0",
	}

	checkRecord(t, records[0], "/text", `a: This is attribute 1 record of record 1
b: This is attribute 2 record of record 1
c: This is attribute 3 record of record 1
d: This is attribute 4 record of record 1
e: This is attribute 5 record of record 1`, expectedHeaders)

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "a.txt"),
		FileName: "a.txt",
		Offset:   "210",
	}

	checkRecord(t, records[1], "/text", `
a: This is attribute 1 record of record 2
b: This is attribute 2 record of record 2
c: This is attribute 3 record of record 2
d: This is attribute 4 record of record 2
e: This is attribute 5 record of record 2`, expectedHeaders)

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "a.txt"),
		FileName: "a.txt",
		Offset:   "421",
	}

	checkRecord(t, records[2], "/text", `
a: This is attribute 1 record of record 3
b: This is attribute 2 record of record 3
c: This is attribute 3 record of record 3
d: This is attribute 4 record of record 3
e: This is attribute 5 record of record 3`, expectedHeaders)
}

func TestCustomDelimiter2(t *testing.T) {
	testCustomDelimiter2(t, dataparser.CompressedNone)
	testCustomDelimiter2(t, dataparser.CompressedFile)
}

func testCustomDelimiter2(t *testing.T, compression string) {
	testDir := createTestDirectory(t)

	defer deleteTestDirectory(t, testDir)

	testData := `# Line1
# Line2
Continued line2
Continued line2
# line3
`
	createFileAndWriteContents(t, filepath.Join(testDir, "a.txt"), testData, compression)
	currentTime := time.Now()

	_ = os.Chtimes(
		filepath.Join(testDir, "a.txt"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(3*time.Second).Nanoseconds()))

	stageConfig := getStageConfig(testDir, false, Regex, "(.*)[.]txt", true, "", 1, "TEXT", compression)

	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.dataFormatConfig.useCustomDelimiter",
		Value: "true",
	})
	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.dataFormatConfig.customDelimiter",
		Value: "\\n#",
	})

	stageContext := createStageContext(stageConfig)

	_, records := createSpoolerAndRun(t, stageContext, "", 3)

	if len(records) != 3 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 2)
	}

	expectedHeaders := map[string]string{
		File:     filepath.Join(testDir, "a.txt"),
		FileName: "a.txt",
		Offset:   "0",
	}

	checkRecord(t, records[0], "/text", `# Line1`, expectedHeaders)

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "a.txt"),
		FileName: "a.txt",
		Offset:   "8",
	}

	checkRecord(t, records[1], "/text", `# Line2
Continued line2
Continued line2`, expectedHeaders)

	expectedHeaders = map[string]string{
		File:     filepath.Join(testDir, "a.txt"),
		FileName: "a.txt",
		Offset:   "48",
	}

	checkRecord(t, records[2], "/text", `# line3`, expectedHeaders)
}

func TestErrorArchiving(t *testing.T) {
	testDir := createTestDirectory(t)
	errorArchiveDir := createTestDirectory(t)

	defer deleteTestDirectory(t, testDir)

	//Create a.txt,c.txt,b.txt with different mod times
	createFileAndWriteContents(t, filepath.Join(testDir, "a.txt"), "{\"text\": \"123\"}\n{\"text\": \"456\"}", dataparser.CompressedNone)
	createFileAndWriteContents(t, filepath.Join(testDir, "b.txt"), "{\"text\": \"111213\"}\n{\"text\": \"141516\"}", dataparser.CompressedFile)
	createFileAndWriteContents(t, filepath.Join(testDir, "c.txt"), "{\"text\": \"111112113\"}\n{\"text\": \"117118119\"}", dataparser.CompressedNone)

	stageConfig := getStageConfig(testDir, false, Glob, "*", false, "", 1, "JSON", dataparser.CompressedFile)
	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.errorArchiveDir",
		Value: errorArchiveDir,
	})
	stageContext := createStageContext(stageConfig)

	offset, records := createSpoolerAndRun(t, stageContext, "", 3)

	if len(records) != 0 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 0)
	}

	// since first file is not compressed one, it should go to error archive directory
	archivedFiles, _ := ioutil.ReadDir(errorArchiveDir)
	if len(archivedFiles) != 1 {
		t.Error("Failed to send 1 error files to error archive directory")
	}

	// second batch
	offset, records = createSpoolerAndRun(t, stageContext, offset, 3)
	if len(records) != 2 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 2)
	}

	// third batch
	offset, records = createSpoolerAndRun(t, stageContext, offset, 3)
	if len(records) != 0 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 0)
	}
	// since third file is not compressed one, it should go to error archive directory
	archivedFiles, _ = ioutil.ReadDir(errorArchiveDir)
	if len(archivedFiles) != 2 {
		t.Error("Failed to send 2 error files to error archive directory")
	}

}

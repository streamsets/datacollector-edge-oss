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
	"bytes"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func createStageContext(
	dirPath string,
	processSubDirectories bool,
	pathMatherMode string,
	filePattern string,
	useLastModified bool,
	initialFileToProcess string,
	pollingTimeoutSeconds int64,
) *common.StageContextImpl {
	stageConfig := common.StageConfiguration{}
	stageConfig.Library = LIBRARY
	stageConfig.StageName = STAGE_NAME
	stageConfig.Configuration = make([]common.Config, 7)

	stageConfig.Configuration[0] = common.Config{
		Name:  SPOOL_DIR_PATH,
		Value: dirPath,
	}

	stageConfig.Configuration[1] = common.Config{
		Name:  PROCESS_SUB_DIRECTORIES,
		Value: processSubDirectories,
	}

	stageConfig.Configuration[2] = common.Config{
		Name:  PATH_MATHER_MODE,
		Value: pathMatherMode,
	}

	stageConfig.Configuration[3] = common.Config{
		Name:  FILE_PATTERN,
		Value: filePattern,
	}

	readOrder := LEXICOGRAPHICAL

	if useLastModified {
		readOrder = TIMESTAMP
	}

	stageConfig.Configuration[4] = common.Config{
		Name:  USE_LAST_MODIFIED,
		Value: readOrder,
	}

	stageConfig.Configuration[5] = common.Config{
		Name:  INITIAL_FILE_TO_PROCESS,
		Value: initialFileToProcess,
	}

	stageConfig.Configuration[6] = common.Config{
		Name:  POLLING_TIMEOUT_SECONDS,
		Value: float64(pollingTimeoutSeconds),
	}

	return &common.StageContextImpl{
		StageConfig: &stageConfig,
		Parameters:  nil,
	}
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

func createFileAndWriteContents(t *testing.T, filePath string, data string) {
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
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters)
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
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{})

	offset, err := stageInstance.(api.Origin).Produce(&lastSourceOffset, batchSize, batchMaker)
	if err != nil {
		t.Fatal("Err :", err)
	}

	stageInstance.Destroy()

	return *offset, batchMaker.GetStageOutput()
}

func checkRecord(
	t *testing.T,
	record api.Record,
	value interface{},
	headersToCheck map[string]string,
) {
	isError := false
	expectedValue := value.(string)

	rootField, _ := record.Get()
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

func TestUseLastModified(t *testing.T) {
	testDir := createTestDirectory(t)

	defer deleteTestDirectory(t, testDir)

	//Create a.txt,c.txt,b.txt with different mod times
	createFileAndWriteContents(t, filepath.Join(testDir, "a.txt"), "123\n456")
	createFileAndWriteContents(t, filepath.Join(testDir, "c.txt"), "111112113\n114115116\n117118119")
	createFileAndWriteContents(t, filepath.Join(testDir, "b.txt"), "111213\n141516")

	currentTime := time.Now()

	os.Chtimes(
		filepath.Join(testDir, "a.txt"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(3*time.Second).Nanoseconds()))
	os.Chtimes(
		filepath.Join(testDir, "c.txt"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(2*time.Second).Nanoseconds()))
	os.Chtimes(
		filepath.Join(testDir, "b.txt"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(time.Second).Nanoseconds()))

	stageContext := createStageContext(testDir, false, REGEX, "(.*)[.]txt", true, "", 1)

	offset, records := createSpoolerAndRun(t, stageContext, "", 3)

	if len(records) != 2 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 2)
	}

	expectedHeaders := map[string]string{
		FILE:      filepath.Join(testDir, "a.txt"),
		FILE_NAME: "a.txt",
		OFFSET:    "0",
	}

	checkRecord(t, records[0], "123", expectedHeaders)

	expectedHeaders = map[string]string{
		FILE:      filepath.Join(testDir, "a.txt"),
		FILE_NAME: "a.txt",
		OFFSET:    "4",
	}

	checkRecord(t, records[1], "456", expectedHeaders)

	offset, records = createSpoolerAndRun(t, stageContext, offset, 2)

	if len(records) != 2 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 2)
	}

	expectedHeaders = map[string]string{
		FILE:      filepath.Join(testDir, "c.txt"),
		FILE_NAME: "c.txt",
		OFFSET:    "0",
	}

	checkRecord(t, records[0], "111112113", expectedHeaders)

	expectedHeaders = map[string]string{
		FILE:      filepath.Join(testDir, "c.txt"),
		FILE_NAME: "c.txt",
		OFFSET:    "10",
	}

	checkRecord(t, records[1], "114115116", expectedHeaders)

	offset, records = createSpoolerAndRun(t, stageContext, offset, 2)

	if len(records) != 1 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 1)
	}

	expectedHeaders = map[string]string{
		FILE:      filepath.Join(testDir, "c.txt"),
		FILE_NAME: "c.txt",
		OFFSET:    "20",
	}

	checkRecord(t, records[0], "117118119", expectedHeaders)

	offset, records = createSpoolerAndRun(t, stageContext, offset, 2)

	if len(records) != 2 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 2)
	}

	expectedHeaders = map[string]string{
		FILE:      filepath.Join(testDir, "b.txt"),
		FILE_NAME: "b.txt",
		OFFSET:    "0",
	}

	checkRecord(t, records[0], "111213", expectedHeaders)

	expectedHeaders = map[string]string{
		FILE:      filepath.Join(testDir, "b.txt"),
		FILE_NAME: "b.txt",
		OFFSET:    "7",
	}

	checkRecord(t, records[1], "141516", expectedHeaders)
}

func TestLexicographical(t *testing.T) {

	testDir := createTestDirectory(t)

	defer deleteTestDirectory(t, testDir)

	//Create a.txt,c.txt,b.txt with different mod times
	createFileAndWriteContents(t, filepath.Join(testDir, "a.txt"), "123\n456")
	createFileAndWriteContents(t, filepath.Join(testDir, "b.txt"), "111213\n141516")
	createFileAndWriteContents(t, filepath.Join(testDir, "c.txt"), "111112113\n114115116\n117118119")

	currentTime := time.Now()

	os.Chtimes(
		filepath.Join(testDir, "a.txt"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(3*time.Second).Nanoseconds()))
	os.Chtimes(
		filepath.Join(testDir, "b.txt"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(2*time.Second).Nanoseconds()))
	os.Chtimes(
		filepath.Join(testDir, "c.txt"),
		currentTime, time.Unix(0, currentTime.UnixNano()-(time.Second).Nanoseconds()))

	stageContext := createStageContext(testDir, false, GLOB, "*", false, "", 1)

	offset, records := createSpoolerAndRun(t, stageContext, "", 3)

	if len(records) != 2 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 2)
	}

	expectedHeaders := map[string]string{
		FILE:      filepath.Join(testDir, "a.txt"),
		FILE_NAME: "a.txt",
		OFFSET:    "0",
	}

	checkRecord(t, records[0], "123", expectedHeaders)

	expectedHeaders = map[string]string{
		FILE:      filepath.Join(testDir, "a.txt"),
		FILE_NAME: "a.txt",
		OFFSET:    "4",
	}

	checkRecord(t, records[1], "456", expectedHeaders)

	offset, records = createSpoolerAndRun(t, stageContext, offset, 2)

	if len(records) != 2 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 2)
	}

	expectedHeaders = map[string]string{
		FILE:      filepath.Join(testDir, "b.txt"),
		FILE_NAME: "b.txt",
		OFFSET:    "0",
	}

	checkRecord(t, records[0], "111213", expectedHeaders)

	expectedHeaders = map[string]string{
		FILE:      filepath.Join(testDir, "b.txt"),
		FILE_NAME: "b.txt",
		OFFSET:    "7",
	}

	checkRecord(t, records[1], "141516", expectedHeaders)

	offset, records = createSpoolerAndRun(t, stageContext, offset, 2)

	if len(records) != 2 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 2)
	}

	expectedHeaders = map[string]string{
		FILE:      filepath.Join(testDir, "c.txt"),
		FILE_NAME: "c.txt",
		OFFSET:    "0",
	}

	checkRecord(t, records[0], "111112113", expectedHeaders)

	expectedHeaders = map[string]string{
		FILE:      filepath.Join(testDir, "c.txt"),
		FILE_NAME: "c.txt",
		OFFSET:    "10",
	}

	checkRecord(t, records[1], "114115116", expectedHeaders)

	offset, records = createSpoolerAndRun(t, stageContext, offset, 2)

	if len(records) != 1 {
		t.Fatalf("Wrong number of records, Actual : %d, Expected : %d ", len(records), 1)
	}

	expectedHeaders = map[string]string{
		FILE:      filepath.Join(testDir, "c.txt"),
		FILE_NAME: "c.txt",
		OFFSET:    "20",
	}

	checkRecord(t, records[0], "117118119", expectedHeaders)
}

func TestSubDirectories(t *testing.T) {
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

	createdFiles := []string{}

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
		createFileAndWriteContents(t, fileToCreate, "sample text")
		os.Chtimes(
			fileToCreate, currentTime,
			time.Unix(0, currentTime.UnixNano()+
				(int64(len(createdFiles))*time.Second.Nanoseconds())))
		createdFiles = append(createdFiles, fileToCreate)
	}

	stageContext := createStageContext(testDir, true, GLOB, "*", true, "", 1)

	var offset string = ""
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
			FILE:      fileToCreate,
			FILE_NAME: filepath.Base(fileToCreate),
			OFFSET:    "0",
		}

		checkRecord(t, records[0], "sample text", expectedHeaders)
	}
}

func TestReadingFileAcrossBatches(t *testing.T) {
	testDir := createTestDirectory(t)
	defer deleteTestDirectory(t, testDir)

	allLetters := []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

	expectedRecordContents := []string{}
	contents := bytes.NewBuffer([]byte{})

	totalLines, totalCharactersInLine := 100, 20

	for line := 0; line < totalLines; line++ {
		var currentLine string = ""
		for lineLetters := 0; lineLetters < totalCharactersInLine; lineLetters++ {
			currentLine = currentLine + string(allLetters[rand.Intn(len(allLetters)-1)])

		}
		expectedRecordContents = append(expectedRecordContents, currentLine)
		contents.WriteString(currentLine + "\n")
	}

	//Create a.txt,c.txt,b.txt with different mod times
	createFileAndWriteContents(t, filepath.Join(testDir, "a.txt"), contents.String())

	stageInstance := createSpooler(t, createStageContext(testDir, false, REGEX, ".*", true, "", 1))
	defer stageInstance.Destroy()

	noOfRecords := 0

	lastSourceOffsetStr := ""
	lastSourceOffset := &lastSourceOffsetStr

	for noOfRecords < totalLines {
		batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{})
		lastSourceOffset, _ = stageInstance.(api.Origin).Produce(lastSourceOffset, rand.Intn(19)+1, batchMaker)
		records := batchMaker.GetStageOutput()
		for rIdx, record := range records {
			checkRecord(t, record, expectedRecordContents[noOfRecords+rIdx], map[string]string{})
		}
		noOfRecords += len(records)
	}

	//No more records to read
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{})
	lastSourceOffset, _ = stageInstance.(api.Origin).Produce(lastSourceOffset, rand.Intn(19)+1, batchMaker)
	if len(batchMaker.GetStageOutput()) != 0 {
		t.Fatal("Read more number of records than expected")
	}
}

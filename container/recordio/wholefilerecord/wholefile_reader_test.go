// Copyright 2019 StreamSets Inc.
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
package wholefilerecord

import (
	"github.com/spf13/cast"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func CreateStageContext() api.StageContext {
	return &common.StageContextImpl{
		StageConfig: &common.StageConfiguration{InstanceName: "Dummy Stage"},
		Parameters:  nil,
	}
}

func TestReadWholeFileRecord(t *testing.T) {
	testReadWholeFileRecord(t, -1)
	testReadWholeFileRecord(t, 100)
}

func testReadWholeFileRecord(t *testing.T, rateLimit float64) {
	sampleData := "Testing Whole File record"
	testDir := createTestDirectory(t)
	defer deleteTestDirectory(t, testDir)
	testWholeFilePath := filepath.Join(testDir, "a.txt")
	createFileAndWriteContents(t, testWholeFilePath, sampleData)

	stageContext := CreateStageContext()

	fileMetadata, err := GetFileInfo(testWholeFilePath)
	if err != nil {
		t.Error(err)
	}

	fileRef := NewLocalFileRef(
		testWholeFilePath,
		1024,
		cast.ToInt(rateLimit),
	)

	readerFactoryImpl := &WholeFileReaderFactoryImpl{}
	recordReader, err := readerFactoryImpl.CreateWholeFileReader(
		stageContext,
		"messageId",
		fileMetadata,
		fileRef,
	)
	if err != nil {
		t.Fatal(err.Error())
	}

	wholeFileRecord, err := recordReader.ReadRecord()
	if err != nil {
		t.Fatal(err)
	}
	if wholeFileRecord == nil {
		t.Fatal("failed to create whole file record")
	}

	fileInfoValue, err := wholeFileRecord.Get(FileInfoFieldPathName)
	if err != nil {
		t.Fatal(err)
	}

	fileInfoFieldVal := fileInfoValue.Value.(map[string]*api.Field)
	if fileInfoFieldVal["file"].Value != testWholeFilePath {
		t.Error("invalid file info path")
	}

	fileRefValue, err := wholeFileRecord.Get(FileRefFieldPathName)
	if err != nil {
		t.Fatal(err)
	}

	fileRefInstance := fileRefValue.Value.(api.FileRef)
	is, err := fileRefInstance.CreateInputStream()
	if err != nil {
		t.Fatal(err)
	}

	wholeFileBytes, err := ioutil.ReadAll(is)
	if err != nil {
		t.Fatal(err)
	}

	if string(wholeFileBytes) != sampleData {
		t.Errorf("Expected: %s, but got: %s", sampleData, string(wholeFileBytes))
	}

	fileRefInstance.CloseInputStream(is)

	_ = recordReader.Close()
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

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
	"github.com/streamsets/datacollector-edge/stages/lib/dataparser"
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"
)

func TestFilePurger_purge(t *testing.T) {
	testDir := createTestDirectory(t)

	defer deleteTestDirectory(t, testDir)

	//Create a.txt,c.txt,b.txt with different mod times
	createFileAndWriteContents(t, filepath.Join(testDir, "a.txt"), "123\n456", dataparser.CompressedNone)
	createFileAndWriteContents(t, filepath.Join(testDir, "b.txt"), "111213\n141516", dataparser.CompressedNone)
	createFileAndWriteContents(t, filepath.Join(testDir, "c.txt"), "111112113\n114115116\n117118119", dataparser.CompressedNone)

	files, _ := ioutil.ReadDir(testDir)
	if len(files) != 3 {
		t.Error("Failed to create test files")
	}

	filePurger := filePurger{archiveDir: testDir, retentionTime: 2 * time.Second}
	time.Sleep(4 * time.Second)
	filePurger.purge()

	archivedFiles, _ := ioutil.ReadDir(testDir)
	if len(archivedFiles) != 0 {
		t.Error("Failed to purge files")
	}
}

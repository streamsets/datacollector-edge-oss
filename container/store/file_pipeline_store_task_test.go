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
package store

import (
	"fmt"
	"github.com/streamsets/datacollector-edge/container/common"
	"io/ioutil"
	"os"
	"testing"
)

func getPipelineStoreTask(t *testing.T, path string) PipelineStoreTask {
	baseDir, err := ioutil.TempDir("", path)
	if err != nil {
		t.Fatal(err)
	}

	err = os.MkdirAll(baseDir+PipelinesFolder, 0777)
	if err != nil {
		t.Fatalf("MkdirAll %q: %s", baseDir+PipelinesFolder, err)
	}

	runtimeInfo := common.RuntimeInfo{
		HttpUrl: "httpUrl",
		BaseDir: baseDir,
	}
	pipelineStoreTask := NewFilePipelineStoreTask(runtimeInfo)

	return pipelineStoreTask
}

func TestFilePipelineStoreTask_GetPipelines(t *testing.T) {
	pipelineStoreTask := getPipelineStoreTask(t, "TestFilePipelineStoreTask_GetPipelines")
	pipelineInfoList, err := pipelineStoreTask.GetPipelines()
	if err != nil {
		t.Error("Error from GetPipelines: ", err)
	}

	if len(pipelineInfoList) != 0 {
		t.Error("Excepted pipelineInfoList with length 0")
	}
}

func TestFilePipelineStoreTask_Create(t *testing.T) {
	pipelineStoreTask := getPipelineStoreTask(t, "TestFilePipelineStoreTask_Create")

	pipelineConfig, err := pipelineStoreTask.Create("testPipeline", "testPipeline", "Sample desc", false)
	if err != nil {
		t.Error("Error from Create: ", err)
		return
	}

	if pipelineConfig.PipelineId != "testPipeline" {
		t.Error("Excepted pipelineId 'testPipeline' but got : ", pipelineConfig.PipelineId)
	}

	if pipelineConfig.Info.PipelineId != "testPipeline" {
		t.Error("Excepted pipelineId 'testPipeline' but got : ", pipelineConfig.PipelineId)
	}

	pipelineInfoList, err := pipelineStoreTask.GetPipelines()
	if err != nil {
		t.Error("Error from GetPipelines: ", err)
		return
	}

	if len(pipelineInfoList) != 1 {
		t.Error("Excepted pipelineInfoList with length 1, but got: ", len(pipelineInfoList))
	}

	// try creating duplicate pipeline
	pipelineConfig, err = pipelineStoreTask.Create("testPipeline", "testPipeline", "Sample desc", false)
	if err == nil {
		t.Error("Excepted error for duplicate pipelineId")
	}
}

func TestFilePipelineStoreTask_Save(t *testing.T) {
	pipelineStoreTask := getPipelineStoreTask(t, "TestFilePipelineStoreTask_Save")

	pipelineConfig, err := pipelineStoreTask.Create("testPipeline", "testPipeline", "Sample desc", false)
	if err != nil {
		t.Error("Error from Create: ", err)
		return
	}

	pipelineConfig.Title = "testPipelineChangeTitle"
	pipelineConfig.Description = "New Description"
	updatedPipelineConfig, err := pipelineStoreTask.Save("testPipeline", pipelineConfig)
	if err != nil {
		t.Error("Error from Create: ", err)
		return
	}

	if updatedPipelineConfig.Title != "testPipelineChangeTitle" {
		t.Error("Excepted pipelineId 'testPipelineChangeTitle' but got : ", pipelineConfig.Title)
	}

	// Save invalid pipelineId
	updatedPipelineConfig, err = pipelineStoreTask.Save("invalidPipeline", pipelineConfig)
	if err == nil {
		t.Error("Error excepted for invalid pipelineId")
	}
}

func TestFilePipelineStoreTask_LoadPipelineConfig(t *testing.T) {
	pipelineStoreTask := getPipelineStoreTask(t, "TestFilePipelineStoreTask_LoadPipelineConfig")

	pipelineConfig, err := pipelineStoreTask.Create("testPipeline", "testPipeline", "Sample desc", false)
	if err != nil {
		t.Error("Error from Create: ", err)
		return
	}

	pipelineConfig, err = pipelineStoreTask.LoadPipelineConfig("testPipeline")

	if pipelineConfig.PipelineId != "testPipeline" {
		t.Error("Excepted pipelineId 'testPipeline' but got : ", pipelineConfig.PipelineId)
	}

	if pipelineConfig.Info.PipelineId != "testPipeline" {
		t.Error("Excepted pipelineId 'testPipeline' but got : ", pipelineConfig.PipelineId)
	}

	// test GetInfo
	pipelineInfo, err := pipelineStoreTask.GetInfo("testPipeline")
	if pipelineInfo.PipelineId != "testPipeline" {
		t.Error("Excepted pipelineId 'testPipeline' but got : ", pipelineConfig.PipelineId)
	}

	// test invalid pipelineId
	pipelineConfig, err = pipelineStoreTask.LoadPipelineConfig("invalidPipeline")
	if err == nil {
		t.Error("Error excepted for invalid pipelineId")
	}

	pipelineInfo, err = pipelineStoreTask.GetInfo("invalidPipeline")
	if err == nil {
		t.Error("Error excepted for invalid pipelineId")
	}

	fmt.Println(err)

}

func TestFilePipelineStoreTask_Delete(t *testing.T) {
	pipelineStoreTask := getPipelineStoreTask(t, "TestFilePipelineStoreTask_Delete")

	pipelineConfig, err := pipelineStoreTask.Create("testDeletePipeline", "testPipeline", "Sample desc", false)
	if err != nil {
		t.Error("Error from Create: ", err)
		return
	}

	if pipelineConfig.PipelineId != "testDeletePipeline" {
		t.Error("Pipeline creation failed ")
		return
	}

	err = pipelineStoreTask.Delete("testDeletePipeline")
	if err != nil {
		t.Error("Error from delete: ", err)
	}
	_, err = pipelineStoreTask.GetInfo("testDeletePipeline")
	if err == nil {
		t.Error("Excepted error from GetInfo after pipeline is deleted")
	}

	pipelineInfoList, err := pipelineStoreTask.GetPipelines()
	if err != nil {
		t.Error("Error from GetPipelines: ", err)
		return
	}

	if len(pipelineInfoList) != 0 {
		t.Error("Excepted pipelineInfoList with length 0, but got: ", len(pipelineInfoList))
	}

	// try deleting non existing pipeline
	err = pipelineStoreTask.Delete("notAValidPipelineId")
	if err == nil {
		t.Error("Excepted error from delete API")
	}
}

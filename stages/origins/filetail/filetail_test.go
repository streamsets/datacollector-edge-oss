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
package filetail

import (
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func getStageContext(filePath string, maxWaitTimeSecs float64, batchSize float64) *common.StageContextImpl {
	stageConfig := common.StageConfiguration{}
	stageConfig.Library = LIBRARY
	stageConfig.StageName = STAGE_NAME
	stageConfig.Configuration = make([]common.Config, 3)

	fileInfoSlice := make([]interface{}, 1, 1)
	fileInfoSlice[0] = map[string]interface{}{
		"fileFullPath": filePath,
	}

	stageConfig.Configuration[0] = common.Config{
		Name:  CONF_FILE_INFOS,
		Value: fileInfoSlice,
	}
	stageConfig.Configuration[1] = common.Config{
		Name:  CONF_BATCH_SIZE,
		Value: batchSize,
	}
	stageConfig.Configuration[2] = common.Config{
		Name:  CONF_MAX_WAIT_TIME_SECS,
		Value: maxWaitTimeSecs,
	}

	return &common.StageContextImpl{
		StageConfig: stageConfig,
		Parameters:  nil,
	}
}

func TestInvalidFilePath(t *testing.T) {
	stageContext := getStageContext("/no/such/file", 2, 1000)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters)
	if err != nil {
		t.Error(err)
		return
	}
	stageInstance := stageBean.Stage
	err = stageInstance.Init(stageContext)
	if err != nil {
		t.Error(err)
	}

	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{})
	_, err = stageInstance.(api.Origin).Produce("", 1000, batchMaker)
	if err == nil {
		t.Error("Excepted error message for invalid URL")
	}
	log.Println("err - ", err)
	stageInstance.Destroy()
}

func TestValidFilePath(t *testing.T) {
	content := []byte("test data 1\ntest data 2\ntest data 3\ntest data 4\n")
	dir, err := ioutil.TempDir("", "TestValidFilePath")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir) // clean up

	filePath := filepath.Join(dir, "tmpFile.log")
	if err := ioutil.WriteFile(filePath, content, 0666); err != nil {
		t.Fatal(err)
	}

	stageContext := getStageContext(filePath, 2, 4)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage
	err = stageInstance.Init(stageContext)
	if err != nil {
		t.Error(err)
	}

	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{})
	lastSourceOffset, err := stageInstance.(api.Origin).Produce("", 1000, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	if lastSourceOffset == "" {
		t.Error("No offset returned :")
	}
	log.Println("offset - " + lastSourceOffset)

	records := batchMaker.GetStageOutput()
	if len(records) != 4 {
		t.Error("Excepted 4 records but got - ", len(records))
	}

	rootField, _ := records[0].Get()
	mapFieldValue := rootField.Value.(map[string]*api.Field)
	if mapFieldValue["text"].Value != "test data 1" {
		t.Error("Excepted 'test data 1' but got - ", rootField.Value)
	}

	// With maxBatchSize 2 - batch 1
	stageInstance.(*FileTailOrigin).Conf.BatchSize = 2
	batchMaker = runner.NewBatchMakerImpl(runner.StagePipe{})
	lastSourceOffset, err = stageInstance.(api.Origin).Produce("", 2, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	records = batchMaker.GetStageOutput()
	if len(records) != 2 {
		t.Error("Excepted 2 records but got - ", len(records))
	}

	rootField, _ = records[0].Get()
	mapFieldValue = rootField.Value.(map[string]*api.Field)
	if mapFieldValue["text"].Value != "test data 1" {
		t.Error("Excepted 'test data 1' but got - ", rootField.Value)
	}

	// With maxBatchSize 2 - batch 2
	batchMaker = runner.NewBatchMakerImpl(runner.StagePipe{})
	lastSourceOffset, err = stageInstance.(api.Origin).Produce(lastSourceOffset, 2, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	records = batchMaker.GetStageOutput()
	if len(records) != 2 {
		t.Error("Excepted 2 records but got - ", len(records))
	}

	rootField, _ = records[0].Get()
	mapFieldValue = rootField.Value.(map[string]*api.Field)
	if mapFieldValue["text"].Value != "test data 3" {
		t.Error("Excepted 'test data 3' but got - ", rootField.Value)
	}

	stageInstance.Destroy()
}

func _TestChannelDeadlockIssue(t *testing.T) {
	filePath1 := "/Users/test/dpm.log"

	stageContext := getStageContext(filePath1, 2, 1000)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage
	err = stageInstance.Init(stageContext)
	if err != nil {
		t.Error(err)
	}

	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{})
	lastSourceOffset, err := stageInstance.(api.Origin).Produce("", 1000, batchMaker)
	log.Println("offset - " + lastSourceOffset)

	lastSourceOffset, err = stageInstance.(api.Origin).Produce(lastSourceOffset, 1000, batchMaker)
	log.Println("offset - " + lastSourceOffset)

	lastSourceOffset, err = stageInstance.(api.Origin).Produce(lastSourceOffset, 1000, batchMaker)
	log.Println("offset - " + lastSourceOffset)

	for true {
		lastSourceOffset, err = stageInstance.(api.Origin).Produce(lastSourceOffset, 1000, batchMaker)
		log.Println("offset - " + lastSourceOffset)
	}

	stageInstance.Destroy()
}

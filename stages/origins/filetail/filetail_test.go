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
package filetail

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
	"github.com/streamsets/datacollector-edge/container/recordio/delimitedrecord"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const sampleCsvData1 = `policyID,statecode,county,eq_site_limit,hu_site_limit,fl_site_limit,fr_site_limit,tiv_2011,tiv_2012,eq_site_deductible,hu_site_deductible,fl_site_deductible,fr_site_deductible,point_latitude,point_longitude,line,construction,point_granularity
119736,FL,CLAY COUNTY,498960,498960,498960,498960,498960,792148.9,0,9979.2,0,0,30.102261,-81.711777,Residential,Masonry,1
448094,FL,CLAY COUNTY,1322376.3,1322376.3,1322376.3,1322376.3,1322376.3,1438163.57,0,0,0,0,30.063936,-81.707664,Residential,Masonry,3
206893,FL,CLAY COUNTY,190724.4,190724.4,190724.4,190724.4,190724.4,192476.78,0,0,0,0,30.089579,-81.700455,Residential,Wood,1
`

func getStageContext(config []common.Config) *common.StageContextImpl {
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
	filePathArr []string,
	maxWaitTimeSecs float64,
	batchSize float64,
	dataFormat string,
	fileRollMode string,
) []common.Config {

	configuration := make([]common.Config, 4)

	fileInfoSlice := make([]interface{}, len(filePathArr))
	for i, filePath := range filePathArr {
		fileInfoSlice[i] = map[string]interface{}{
			"fileFullPath":    filePath,
			"fileRollMode":    fileRollMode,
			"patternForToken": ".*",
		}
	}

	configuration[0] = common.Config{
		Name:  ConfFileInfos,
		Value: fileInfoSlice,
	}
	configuration[1] = common.Config{
		Name:  ConfBatchSize,
		Value: batchSize,
	}
	configuration[2] = common.Config{
		Name:  ConfMaxWaitTimeSecs,
		Value: maxWaitTimeSecs,
	}
	configuration[3] = common.Config{
		Name:  ConfDataFormat,
		Value: dataFormat,
	}

	return configuration
}

func TestInvalidFilePath(t *testing.T) {
	stageContext := getStageContext(getStageConfig([]string{"/no/such/file"}, 2, 1000, "TEXT", FileRollModeReverseCounter))
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}
	stageInstance := stageBean.Stage
	issues := stageInstance.Init(stageContext)
	if len(issues) == 0 {
		t.Error("Expected File path doesn't exist error")
		return
	}

	if !strings.Contains(issues[0].Message, "File path doesn't exist") {
		t.Errorf("Expected 'File path doesn't exist', but got: %s", issues[0].Message)
	}
}

func TestPatternFileRollModeWithoutPatternToken(t *testing.T) {
	fileInfoSlice := []interface{}{
		map[string]interface{}{
			"fileFullPath":    "/var/log/sample.log",
			"fileRollMode":    "PATTERN",
			"patternForToken": "",
		},
	}
	configuration := []common.Config{
		{
			Name:  ConfFileInfos,
			Value: fileInfoSlice,
		},
	}

	stageContext := getStageContext(configuration)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}
	stageInstance := stageBean.Stage
	issues := stageInstance.Init(stageContext)
	if len(issues) == 0 {
		t.Error("Expected File path doesn't exist error")
		return
	}

	if !strings.Contains(issues[0].Message, "requires the '${PATTERN}' token in the") {
		t.Errorf("Expected 'requires the '${PATTERN}' token in the ', but got: %s", issues[0].Message)
	}
}

func TestValidFilePath(t *testing.T) {
	content := []byte("test data 1 extra text\ntest data 2\ntest data 3\ntest data 4\n")
	dir, err := ioutil.TempDir("", "TestValidFilePath")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir) // clean up

	filePath := filepath.Join(dir, "tmpFile.log")
	if err := ioutil.WriteFile(filePath, content, 0666); err != nil {
		t.Fatal(err)
	}

	stageConfig := getStageConfig([]string{filePath}, 2, 4, "TEXT", FileRollModeReverseCounter)
	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.dataFormatConfig.textMaxLineLen",
		Value: float64(11),
	})

	stageContext := getStageContext(stageConfig)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage
	issues := stageInstance.Init(stageContext)
	if len(issues) != 0 {
		t.Error(issues[0].Message)
	}

	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	lastSourceOffset, err := stageInstance.(api.Origin).Produce(nil, 1000, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	if lastSourceOffset == nil {
		t.Error("No offset returned :")
	}
	log.Println("offset - " + *lastSourceOffset)

	records := batchMaker.GetStageOutput()
	if len(records) != 4 {
		t.Fatal("Excepted 4 records but got - ", len(records))
	}

	rootField, _ := records[0].Get()
	mapFieldValue := rootField.Value.(map[string]*api.Field)
	if mapFieldValue["text"].Value != "test data 1" {
		t.Error("Excepted 'test data 1' but got - ", rootField.Value)
	}

	// With maxBatchSize 2 - batch 1
	stageInstance.(*FileTailOrigin).Conf.BatchSize = 2
	batchMaker = runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	lastSourceOffset, err = stageInstance.(api.Origin).Produce(nil, 2, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	records = batchMaker.GetStageOutput()
	if len(records) != 2 {
		t.Fatal("Excepted 2 records but got - ", len(records))
	}

	rootField, _ = records[0].Get()
	mapFieldValue = rootField.Value.(map[string]*api.Field)
	if mapFieldValue["text"].Value != "test data 1" {
		t.Error("Excepted 'test data 1' but got - ", rootField.Value)
	}

	// With maxBatchSize 2 - batch 2
	batchMaker = runner.NewBatchMakerImpl(runner.StagePipe{}, false)
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

	// make sure calling stop and start doesn't cause any deadlock
	lastSourceOffset, err = stageInstance.(api.Origin).Produce(nil, 2, batchMaker)
	lastSourceOffset, err = stageInstance.(api.Origin).Produce(nil, 2, batchMaker)
	lastSourceOffset, err = stageInstance.(api.Origin).Produce(nil, 2, batchMaker)
	lastSourceOffset, err = stageInstance.(api.Origin).Produce(nil, 2, batchMaker)

	_ = stageInstance.Destroy()
}

func TestMultipleFilePath(t *testing.T) {
	content1 := []byte("test data 1\ntest data 2\ntest data 3\ntest data 4\n")
	content2 := []byte("sample data 1\nsample data 2\nsample data 3\nsample data 4\n")
	dir, err := ioutil.TempDir("", "TestValidFilePath")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir) // clean up

	filePath1 := filepath.Join(dir, "tmpFile1.log")
	if err := ioutil.WriteFile(filePath1, content1, 0666); err != nil {
		t.Fatal(err)
	}

	filePath2 := filepath.Join(dir, "tmpFile2.log")
	if err := ioutil.WriteFile(filePath2, content2, 0666); err != nil {
		t.Fatal(err)
	}

	stageConfig := getStageConfig([]string{filePath1, filePath2}, 2, 4, "TEXT", FileRollModeReverseCounter)
	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.dataFormatConfig.textMaxLineLen",
		Value: float64(1024),
	})

	stageContext := getStageContext(stageConfig)

	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage
	issues := stageInstance.Init(stageContext)
	if len(issues) != 0 {
		t.Error(issues[0].Message)
	}

	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	lastSourceOffset, err := stageInstance.(api.Origin).Produce(nil, 1000, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	if lastSourceOffset == nil {
		t.Error("No offset returned :")
	}
	log.Println("offset - " + *lastSourceOffset)

	records := batchMaker.GetStageOutput()
	if len(records) != 4 {
		t.Fatal("Excepted 4 records but got - ", len(records))
	}

	// With maxBatchSize 2 - batch 1
	stageInstance.(*FileTailOrigin).Conf.BatchSize = 2
	batchMaker = runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	lastSourceOffset, err = stageInstance.(api.Origin).Produce(nil, 2, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	records = batchMaker.GetStageOutput()
	if len(records) != 2 {
		t.Fatal("Excepted 2 records but got - ", len(records))
	}

	// Get all records from both file - With maxBatchSize 20 - batch 1
	stageInstance.(*FileTailOrigin).Conf.BatchSize = 20
	batchMaker = runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	lastSourceOffset, err = stageInstance.(api.Origin).Produce(nil, 20, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	records = batchMaker.GetStageOutput()
	if len(records) != 4 {
		t.Fatal("Excepted 4 records but got - ", len(records))
	}

	rootField, _ := records[0].Get()
	mapFieldValue := rootField.Value.(map[string]*api.Field)
	if mapFieldValue["text"].Value != "test data 1" {
		t.Error("Excepted 'test data 1' but got - ", rootField.Value)
	}

	// second batch should get lines from second file
	batchMaker = runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	lastSourceOffset, err = stageInstance.(api.Origin).Produce(lastSourceOffset, 20, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}
	log.Println("offset - " + *lastSourceOffset)

	records = batchMaker.GetStageOutput()
	if len(records) != 4 {
		t.Fatal("Excepted 4 records but got - ", len(records))
	}

	rootField, _ = records[0].Get()
	mapFieldValue = rootField.Value.(map[string]*api.Field)
	if mapFieldValue["text"].Value != "sample data 1" {
		t.Error("Excepted 'sample data 1' but got - ", mapFieldValue["text"].Value)
	}

	_ = stageInstance.Destroy()
}

func TestFilePathRegexWithPattern(t *testing.T) {
	content1 := []byte("test data 1\ntest data 2\ntest data 3\ntest data 4\n")
	content2 := []byte("sample data 1\nsample data 2\nsample data 3\n")
	dir1, err := ioutil.TempDir("", "dir1")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir1) // clean up

	filePath1 := filepath.Join(dir1, "tmpFile1.log")
	if err := ioutil.WriteFile(filePath1, content1, 0666); err != nil {
		t.Fatal(err)
	}

	filePath2 := filepath.Join(dir1, "tmpFile2.log")
	if err := ioutil.WriteFile(filePath2, content2, 0666); err != nil {
		t.Fatal(err)
	}

	regexPath := filepath.Join(dir1, "tmpFile*${PATTERN}")
	regexPath = strings.Replace(regexPath, "dir1", "*", 1)

	stageConfig := getStageConfig([]string{regexPath}, 2, 500, "TEXT", FileRollModePattern)
	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.dataFormatConfig.textMaxLineLen",
		Value: float64(1024),
	})
	stageContext := getStageContext(stageConfig)

	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage
	issues := stageInstance.Init(stageContext)
	if len(issues) != 0 {
		t.Error(issues[0].Message)
	}

	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	lastSourceOffset, err := stageInstance.(api.Origin).Produce(nil, 1000, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	if lastSourceOffset == nil {
		t.Error("No offset returned :")
	}
	log.Println("offset - " + *lastSourceOffset)

	records := batchMaker.GetStageOutput()
	if len(records) != 4 {
		t.Fatal("Excepted 4 records but got - ", len(records))
	}

	rootField, _ := records[0].Get()
	mapFieldValue := rootField.Value.(map[string]*api.Field)
	if mapFieldValue["text"].Value != "test data 1" {
		t.Error("Excepted 'test data 1' but got - ", rootField.Value)
	}

	// second batch should get lines from first file with 0 records
	batchMaker = runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	lastSourceOffset, err = stageInstance.(api.Origin).Produce(lastSourceOffset, 20, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	records = batchMaker.GetStageOutput()
	if len(records) != 0 {
		t.Fatal("Excepted 0 records but got - ", len(records))
	}

	// third batch should get lines from second file
	batchMaker = runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	lastSourceOffset, err = stageInstance.(api.Origin).Produce(lastSourceOffset, 20, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	records = batchMaker.GetStageOutput()
	if len(records) != 3 {
		t.Fatal("Excepted 3 records but got - ", len(records))
	}

	rootField, _ = records[0].Get()
	mapFieldValue = rootField.Value.(map[string]*api.Field)
	if mapFieldValue["text"].Value != "sample data 1" {
		t.Error("Excepted 'sample data 1' but got - ", rootField.Value)
	}

	_ = stageInstance.Destroy()
}

func TestFileTailOrigin_Produce_JSON(t *testing.T) {
	content := []byte(
		"{\"temperature_C\": \"12.34\", \"pressure_KPa\": \"567.89\", \"humidity\": \"534.44\"}" + "\r\n" +
			"{\"temperature_C\": \"13.34\", \"pressure_KPa\": \"667.89\", \"humidity\": \"634.44\"}" + "\r\n" +
			"{\"temperature_C\": \"14.34\", \"pressure_KPa\": \"767.89\", \"humidity\": \"734.44\"}" + "\r\n",
	)
	dir, err := ioutil.TempDir("", "TestFileTailOrigin_Produce_JSON")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir) // clean up

	filePath := filepath.Join(dir, "tmpFile.log")
	if err := ioutil.WriteFile(filePath, content, 0666); err != nil {
		t.Fatal(err)
	}

	stageContext := getStageContext(getStageConfig([]string{filePath}, 2, 4, "JSON", FileRollModeReverseCounter))
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage
	issues := stageInstance.Init(stageContext)
	if len(issues) != 0 {
		t.Error(issues[0].Message)
	}

	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	lastSourceOffset, err := stageInstance.(api.Origin).Produce(nil, 1000, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	if lastSourceOffset == nil {
		t.Error("No offset returned :")
	}
	log.Println("offset - " + *lastSourceOffset)

	records := batchMaker.GetStageOutput()
	if len(records) != 3 {
		t.Error("Excepted 3 records but got - ", len(records))
	}

	rootField, _ := records[0].Get()
	mapFieldValue := rootField.Value.(map[string]*api.Field)
	if mapFieldValue["temperature_C"].Value != "12.34" {
		t.Error("Excepted '12.34' but got - ", rootField.Value)
	}

	if mapFieldValue["pressure_KPa"].Value != "567.89" {
		t.Error("Excepted '567.89' but got - ", rootField.Value)
	}

	if mapFieldValue["humidity"].Value != "534.44" {
		t.Error("Excepted '534.44' but got - ", rootField.Value)
	}

	_ = stageInstance.Destroy()
}

func TestFileTailOrigin_Produce_DELIMITED_NO_HEADER(t *testing.T) {
	content := []byte(sampleCsvData1)
	dir, err := ioutil.TempDir("", "TestFileTailOrigin_Produce_DELIMITED")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir) // clean up

	filePath := filepath.Join(dir, "tmpFile.csv")
	if err := ioutil.WriteFile(filePath, content, 0666); err != nil {
		t.Fatal(err)
	}

	stageConfig := getStageConfig([]string{filePath}, 2, 4, "DELIMITED", FileRollModeReverseCounter)
	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.dataFormatConfig.csvRecordType",
		Value: delimitedrecord.ListMap,
	})
	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.dataFormatConfig.csvHeader",
		Value: delimitedrecord.NoHeader,
	})

	stageContext := getStageContext(stageConfig)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage
	issues := stageInstance.Init(stageContext)
	if len(issues) != 0 {
		t.Error(issues[0].Message)
	}

	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	lastSourceOffset, err := stageInstance.(api.Origin).Produce(nil, 1000, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	if lastSourceOffset == nil {
		t.Error("No offset returned :")
	}
	log.Println("offset - " + *lastSourceOffset)

	records := batchMaker.GetStageOutput()
	if len(records) != 4 {
		t.Error("Excepted 4 records but got - ", len(records))
	}

	cell1Value, _ := records[0].Get("/0")
	if cell1Value.Value != "policyID" {
		t.Error("Excepted 'policyID' but got - ", cell1Value.Value)
	}

	cell2Value, _ := records[0].Get("/1")
	if cell2Value.Value != "statecode" {
		t.Error("Excepted 'statecode' but got - ", cell2Value.Value)
	}

	cell3Value, _ := records[0].Get("/2")
	if cell3Value.Value != "county" {
		t.Error("Excepted 'county' but got - ", cell3Value.Value)
	}

	_ = stageInstance.Destroy()
}

func TestFileTailOrigin_Produce_DELIMITED_With_HEADER(t *testing.T) {
	content := []byte(sampleCsvData1)
	dir, err := ioutil.TempDir("", "TestFileTailOrigin_Produce_DELIMITED_With_HEADER")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir) // clean up

	filePath := filepath.Join(dir, "tmpFile.csv")
	if err := ioutil.WriteFile(filePath, content, 0666); err != nil {
		t.Fatal(err)
	}

	stageConfig := getStageConfig([]string{filePath}, 2, 5, "DELIMITED", FileRollModeReverseCounter)
	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.dataFormatConfig.csvRecordType",
		Value: delimitedrecord.ListMap,
	})
	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.dataFormatConfig.csvHeader",
		Value: delimitedrecord.WithHeader,
	})

	stageContext := getStageContext(stageConfig)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage
	issues := stageInstance.Init(stageContext)
	if len(issues) != 0 {
		t.Error(issues[0].Message)
	}

	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	lastSourceOffset, err := stageInstance.(api.Origin).Produce(nil, 1000, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	if lastSourceOffset == nil {
		t.Error("No offset returned :")
	}
	log.Println("offset - " + *lastSourceOffset)

	records := batchMaker.GetStageOutput()
	if len(records) != 3 {
		t.Error("Excepted 3 records but got - ", len(records))
	}

	cell1Value, _ := records[2].Get("/policyID")
	if cell1Value.Value != "206893" {
		t.Error("Excepted '206893' but got - ", cell1Value.Value)
	}

	cell2Value, _ := records[2].Get("/statecode")
	if cell2Value.Value != "FL" {
		t.Error("Excepted 'FL' but got - ", cell2Value.Value)
	}

	cell3Value, _ := records[2].Get("/county")
	if cell3Value.Value != "CLAY COUNTY" {
		t.Error("Excepted 'CLAY COUNTY' but got - ", cell3Value.Value)
	}

	_ = stageInstance.Destroy()
}

func TestFileTailOrigin_Produce_DELIMITED_IGNORE_HEADER(t *testing.T) {
	content := []byte(sampleCsvData1)
	dir, err := ioutil.TempDir("", "TestFileTailOrigin_Produce_DELIMITED_IGNORE_HEADER")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir) // clean up

	filePath := filepath.Join(dir, "tmpFile.csv")
	if err := ioutil.WriteFile(filePath, content, 0666); err != nil {
		t.Fatal(err)
	}

	stageConfig := getStageConfig([]string{filePath}, 2, 5, "DELIMITED", FileRollModeReverseCounter)
	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.dataFormatConfig.csvRecordType",
		Value: delimitedrecord.ListMap,
	})
	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.dataFormatConfig.csvHeader",
		Value: delimitedrecord.IgnoreHeader,
	})

	stageContext := getStageContext(stageConfig)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage
	issues := stageInstance.Init(stageContext)
	if len(issues) != 0 {
		t.Error(issues[0].Message)
	}

	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	lastSourceOffset, err := stageInstance.(api.Origin).Produce(nil, 1000, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	if lastSourceOffset == nil {
		t.Error("No offset returned :")
	}
	log.Println("offset - " + *lastSourceOffset)

	records := batchMaker.GetStageOutput()
	if len(records) != 3 {
		t.Fatal("Excepted 3 records but got - ", len(records))
	}

	cell1Value, _ := records[0].Get("/0")
	if cell1Value.Value != "119736" {
		t.Error("Excepted '119736' but got - ", cell1Value.Value)
	}

	cell2Value, _ := records[0].Get("/1")
	if cell2Value.Value != "FL" {
		t.Error("Excepted 'FL' but got - ", cell2Value.Value)
	}

	cell3Value, _ := records[0].Get("/2")
	if cell3Value.Value != "CLAY COUNTY" {
		t.Error("Excepted 'CLAY COUNTY' but got - ", cell3Value.Value)
	}

	_ = stageInstance.Destroy()
}

func TestFileTailOrigin_Produce_DELIMITED_SKIP_LINES(t *testing.T) {
	content := []byte(sampleCsvData1)
	dir, err := ioutil.TempDir("", "TestFileTailOrigin_Produce_DELIMITED_SKIP_LINES")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir) // clean up

	filePath := filepath.Join(dir, "tmpFile.csv")
	if err := ioutil.WriteFile(filePath, content, 0666); err != nil {
		t.Fatal(err)
	}

	stageConfig := getStageConfig([]string{filePath}, 2, 5, "DELIMITED", FileRollModeReverseCounter)
	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.dataFormatConfig.csvRecordType",
		Value: delimitedrecord.ListMap,
	})
	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.dataFormatConfig.csvHeader",
		Value: delimitedrecord.NoHeader,
	})
	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.dataFormatConfig.csvSkipStartLines",
		Value: float64(3),
	})

	stageContext := getStageContext(stageConfig)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage
	issues := stageInstance.Init(stageContext)
	if len(issues) != 0 {
		t.Error(issues[0].Message)
	}

	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	lastSourceOffset, err := stageInstance.(api.Origin).Produce(nil, 1000, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	if lastSourceOffset == nil {
		t.Error("No offset returned :")
	}
	log.Println("offset - " + *lastSourceOffset)

	records := batchMaker.GetStageOutput()
	if len(records) != 1 {
		t.Fatal("Excepted 1 records but got - ", len(records))
	}

	cell1Value, _ := records[0].Get("/0")
	if cell1Value.Value != "206893" {
		t.Error("Excepted '206893' but got - ", cell1Value.Value)
	}

	cell2Value, _ := records[0].Get("/1")
	if cell2Value.Value != "FL" {
		t.Error("Excepted 'FL' but got - ", cell2Value.Value)
	}

	cell3Value, _ := records[0].Get("/2")
	if cell3Value.Value != "CLAY COUNTY" {
		t.Error("Excepted 'CLAY COUNTY' but got - ", cell3Value.Value)
	}

	_ = stageInstance.Destroy()
}

func _TestChannelDeadlockIssue(t *testing.T) {
	filePath1 := "/Users/test/dpm.log"

	stageContext := getStageContext(getStageConfig([]string{filePath1}, 2, 1000, "TEXT", FileRollModeReverseCounter))
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage
	issues := stageInstance.Init(stageContext)
	if len(issues) != 0 {
		t.Error(issues[0].Message)
	}

	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	lastSourceOffset, err := stageInstance.(api.Origin).Produce(nil, 1000, batchMaker)
	log.Println("offset - " + *lastSourceOffset)

	lastSourceOffset, err = stageInstance.(api.Origin).Produce(lastSourceOffset, 1000, batchMaker)
	log.Println("offset - " + *lastSourceOffset)

	lastSourceOffset, err = stageInstance.(api.Origin).Produce(lastSourceOffset, 1000, batchMaker)
	log.Println("offset - " + *lastSourceOffset)

	for true {
		lastSourceOffset, err = stageInstance.(api.Origin).Produce(lastSourceOffset, 1000, batchMaker)
		log.Println("offset - " + *lastSourceOffset)
	}

	_ = stageInstance.Destroy()
}

func TestFileTailOrigin_offsetIssue(t *testing.T) {
	dir, err := ioutil.TempDir("", "TestValidFilePath")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir) // clean up
	filePath := filepath.Join(dir, "offsetTest.jso")
	f, err := os.Create(filePath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	for i := 0; i < 100; i++ {
		_, _ = f.WriteString("{\"count\": \"34\", \"total\": \"45\", \"page\": \"" + strconv.Itoa(i) + "\"}\n")
	}

	stageContext := getStageContext(getStageConfig([]string{filePath}, 2, 10, "JSON", FileRollModeReverseCounter))
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage
	issues := stageInstance.Init(stageContext)
	if len(issues) != 0 {
		t.Error(issues[0].Message)
	}

	lastSourceOffsetStr := ""
	lastSourceOffset := &lastSourceOffsetStr
	recordsCount := int64(0)
	for i := 0; i < 11; i++ {
		batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
		lastSourceOffset, err = stageInstance.(api.Origin).Produce(lastSourceOffset, 10, batchMaker)
		recordsCount += batchMaker.GetSize()
		if err != nil {
			t.Error(err)
		}
	}

	if recordsCount != 100 {
		t.Error("Missed some records, expected 100 but got ", recordsCount)
	}

	_ = stageInstance.Destroy()
}

func TestFilesMatchingAPattern(t *testing.T) {
	testTempDir, err := ioutil.TempDir("", "TestFilesMatchingAPattern")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(testTempDir) // clean up

	numberOfLines := 4

	dirNameArr := []string{"W3SVC40", "W3SVC50"}
	fil1NameArr := []string{"ex_181126.log", "ex_181127.log", "ex_181128.log", "ex_181125.log"}

	for _, dirName := range dirNameArr {
		dirPath := filepath.Join(testTempDir, dirName)
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			log.Fatal("Cannot create directory", err)
		}

		for _, fileName := range fil1NameArr {
			filePath := filepath.Join(dirPath, fileName)
			file, err := os.Create(filePath)
			if err != nil {
				log.Fatal("Cannot create file", err)
			}

			for i := 1; i <= numberOfLines; i++ {
				_, _ = fmt.Fprintf(file, "%s sample data %d\n", filePath, i)
			}

			_ = file.Close()
		}
	}

	regexFilePath := filepath.Join(testTempDir, "W3SVC*", "ex_*.log")

	allFilePaths, _ := filepath.Glob(regexFilePath)
	for _, filePath := range allFilePaths {
		fmt.Println(filePath)
	}

	stageConfig := getStageConfig([]string{regexFilePath}, 2, 500, "TEXT", FileRollModePattern)
	stageConfig = append(stageConfig, common.Config{
		Name:  "conf.dataFormatConfig.textMaxLineLen",
		Value: float64(1024),
	})
	stageContext := getStageContext(stageConfig)

	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage
	issues := stageInstance.Init(stageContext)
	if len(issues) != 0 {
		t.Error(issues[0].Message)
	}

	// first batch should read 4 records from W3SVC40/ex_181125.log
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	lastSourceOffset, err := stageInstance.(api.Origin).Produce(nil, 1000, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	if lastSourceOffset == nil {
		t.Error("No offset returned :")
	}
	log.Println("offset - " + *lastSourceOffset)

	records := batchMaker.GetStageOutput()
	if len(records) != 4 {
		t.Fatal("Excepted 4 records but got - ", len(records))
	}

	rootField, _ := records[0].Get()
	mapFieldValue := rootField.Value.(map[string]*api.Field)
	if mapFieldValue["text"].Value != allFilePaths[0]+" sample data 1" {
		t.Fatalf("Excepted '%s' but got '%s ", allFilePaths[0]+" sample data 1", mapFieldValue["text"].Value)
	}

	// second batch should read 4 records from W3SVC50/ex_181125.log
	batchMaker = runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	lastSourceOffset, err = stageInstance.(api.Origin).Produce(lastSourceOffset, 20, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	records = batchMaker.GetStageOutput()
	if len(records) != 4 {
		t.Fatal("Excepted 4 records but got - ", len(records))
	}

	rootField, _ = records[0].Get()
	mapFieldValue = rootField.Value.(map[string]*api.Field)
	if mapFieldValue["text"].Value != allFilePaths[4]+" sample data 1" {
		t.Fatalf("Excepted '%s' but got '%s ", allFilePaths[4]+" sample data 1", mapFieldValue["text"].Value)
	}

	// third batch should read 0 records from W3SVC40/ex_181125.log
	batchMaker = runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	lastSourceOffset, err = stageInstance.(api.Origin).Produce(lastSourceOffset, 20, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	records = batchMaker.GetStageOutput()
	if len(records) != 0 {
		t.Fatal("Excepted 0 records but got - ", len(records))
	}

	// fourth batch should read 0 records from W3SVC50/ex_181125.log
	batchMaker = runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	lastSourceOffset, err = stageInstance.(api.Origin).Produce(lastSourceOffset, 1000, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	if lastSourceOffset == nil {
		t.Error("No offset returned :")
	}
	log.Println("offset - " + *lastSourceOffset)

	records = batchMaker.GetStageOutput()
	if len(records) != 4 {
		t.Fatal("Excepted 4 records but got - ", len(records))
	}

	rootField, _ = records[0].Get()
	mapFieldValue = rootField.Value.(map[string]*api.Field)
	if mapFieldValue["text"].Value != allFilePaths[1]+" sample data 1" {
		t.Fatalf("Excepted '%s' but got '%s ", allFilePaths[1]+" sample data 1", mapFieldValue["text"].Value)
	}

	// sixth batch should read 4 records from W3SVC40/ex_181126.log
	batchMaker = runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	lastSourceOffset, err = stageInstance.(api.Origin).Produce(lastSourceOffset, 20, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	batchMaker = runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	lastSourceOffset, err = stageInstance.(api.Origin).Produce(lastSourceOffset, 20, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	records = batchMaker.GetStageOutput()
	if len(records) != 4 {
		t.Fatal("Excepted 4 records but got - ", len(records))
	}

	rootField, _ = records[0].Get()
	mapFieldValue = rootField.Value.(map[string]*api.Field)
	if mapFieldValue["text"].Value != allFilePaths[5]+" sample data 1" {
		t.Fatalf("Excepted '%s' but got '%s ", allFilePaths[5]+" sample data 1", mapFieldValue["text"].Value)
	}
}

// Run below command to create test data before running benchmark tests
// for ((i=1;i<=200000;i++)) ; do echo "Hello World" + $i >> /tmp/testforloop.txt ;  done
// Run Benchmark Tests
//    go test -run=^$ -bench=. -memprofile=mem0.out -cpuprofile=cpu0.out
// Profile CPU
// 	  go tool pprof bench.test cpu0.out
// Profile Memory
// 	  go tool pprof --alloc_space bench.test mem0.out
func BenchmarkFileTailOrigin_Produce(b *testing.B) {
	filePath1 := "/tmp/testforloop.txt"

	stageContext := getStageContext(getStageConfig([]string{filePath1}, 2, 1000, "TEXT", FileRollModeReverseCounter))
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		panic(err)
	}
	stageInstance := stageBean.Stage
	issues := stageInstance.Init(stageContext)
	if len(issues) != 0 {
		panic(issues[0].Message)
	}

	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	lastSourceOffset, err := stageInstance.(api.Origin).Produce(nil, 1000, batchMaker)
	log.Println("offset - " + *lastSourceOffset)

	lastSourceOffset, err = stageInstance.(api.Origin).Produce(lastSourceOffset, 1000, batchMaker)
	log.Println("offset - " + *lastSourceOffset)

	lastSourceOffset, err = stageInstance.(api.Origin).Produce(lastSourceOffset, 1000, batchMaker)
	log.Println("offset - " + *lastSourceOffset)

	for i := 0; i < 200; i++ {
		lastSourceOffset, err = stageInstance.(api.Origin).Produce(lastSourceOffset, 1000, batchMaker)
		log.Println("offset - " + *lastSourceOffset)
	}

	_ = stageInstance.Destroy()
}

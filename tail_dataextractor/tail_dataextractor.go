package tail_dataextractor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/hpcloud/tail"
	"github.com/streamsets/dataextractor/container/common"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
)

const DEBUG = false

type TailDataExtractor struct {
	logger         *log.Logger
	pipelineConfig common.PipelineConfiguration
	tail           *tail.Tail
	fileFullPath   string
	resourceUrl    string
	headers        []interface{}
}

func (tailDataExtractor *TailDataExtractor) init() {
	pipelineConfig, err1 := loadPipelineConfig()
	if err1 != nil {
		panic(err1)
	}
	tailDataExtractor.pipelineConfig = pipelineConfig

	tailDataExtractor.initFileTailStage()
	tailDataExtractor.initHttpTargetStage()
}

func (tailDataExtractor *TailDataExtractor) initFileTailStage() {
	fileTailStageInstance := tailDataExtractor.pipelineConfig.Stages[0]

	var fileInfosConfigValue []interface{}
	for _, config := range fileTailStageInstance.Configuration {
		if config.Name == "conf.fileInfos" {
			fileInfosConfigValue = config.Value.([]interface{})
		}
	}

	if fileInfosConfigValue == nil {
		panic("Config conf.fileInfos not found")
	}

	for _, fileInfos := range fileInfosConfigValue {
		tailDataExtractor.fileFullPath = fileInfos.(map[string]interface{})["fileFullPath"].(string)
	}
}

func (tailDataExtractor *TailDataExtractor) initHttpTargetStage() {
	for _, stageInstances := range tailDataExtractor.pipelineConfig.Stages {
		if stageInstances.StageName == "com_streamsets_pipeline_stage_destination_http_HttpClientDTarget" {
			for _, config := range stageInstances.Configuration {
				if config.Name == "conf.resourceUrl" {
					tailDataExtractor.resourceUrl = config.Value.(string)
				}

				if config.Name == "conf.headers" {
					tailDataExtractor.headers = config.Value.([]interface{})
				}
			}
			break
		}
	}

	if tailDataExtractor.resourceUrl == "" {
		panic("Config conf.resourceUrl not found")
	}
}

func (tailDataExtractor *TailDataExtractor) Start(offset string) {
	fmt.Println("Started tailing file: " + tailDataExtractor.fileFullPath)

	tailConfig := tail.Config{Follow: true}

	if offset != "" {
		intOffset, _ := strconv.ParseInt(offset, 10, 64)
		tailConfig.Location = &tail.SeekInfo{Offset: intOffset}
		fmt.Println("Started Offset: ")
		fmt.Println(tailConfig.Location.Offset)
	}

	t, err := tail.TailFile(tailDataExtractor.fileFullPath, tailConfig)

	if err != nil {
		fmt.Println("error:", err)
		panic(err)
	}

	tailDataExtractor.tail = t

	for line := range t.Lines {
		tailDataExtractor.sendLineToSDC(line.Text)
	}
}

func (tailDataExtractor *TailDataExtractor) sendLineToSDC(line string) {
	if DEBUG {
		fmt.Println("Start sending line")
		fmt.Println(line)
		fmt.Println("URL:>", tailDataExtractor.resourceUrl)
	}

	var logTextStr = []byte(line)
	req, err := http.NewRequest("POST", tailDataExtractor.resourceUrl, bytes.NewBuffer(logTextStr))

	if tailDataExtractor.headers != nil {
		for _, header := range tailDataExtractor.headers {
			req.Header.Set(header.(map[string]interface{})["key"].(string),
				header.(map[string]interface{})["value"].(string))
		}
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	if DEBUG {
		fmt.Println("response Status:", resp.Status)
		fmt.Println("response Headers:", resp.Header)
		body, _ := ioutil.ReadAll(resp.Body)
		fmt.Println("response Body:", string(body))
	}
}

func (tailDataExtractor *TailDataExtractor) Stop() (string, error) {
	fmt.Println("Stopping TailDataExtractor ....")

	offset, _ := tailDataExtractor.tail.Tell()

	err := tailDataExtractor.tail.Stop()
	if err != nil {
		fmt.Println("Stop error:", err)
	}

	return strconv.FormatInt(offset, 10), err
}

func New(logger *log.Logger) (*TailDataExtractor, error) {
	tailDataExtractor := TailDataExtractor{logger: logger}
	tailDataExtractor.init()
	return &tailDataExtractor, nil
}

func loadPipelineConfig() (common.PipelineConfiguration, error) {
	pipelineConfiguration := common.PipelineConfiguration{}
	file, err := os.Open("etc/pipeline.json")
	if err != nil {
		return pipelineConfiguration, err
	}

	decoder := json.NewDecoder(file)
	err1 := decoder.Decode(&pipelineConfiguration)
	if err1 != nil {
		return pipelineConfiguration, err1
	}

	if DEBUG {
		fmt.Println("Using Pipeline Configuration")
		fmt.Println(pipelineConfiguration)
	}
	return pipelineConfiguration, err1
}

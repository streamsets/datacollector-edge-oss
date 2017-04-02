package dataextractor

import (
	"github.com/streamsets/dataextractor/container/common"
	"github.com/streamsets/dataextractor/container/dpm"
	"github.com/streamsets/dataextractor/container/execution/manager"
	"github.com/streamsets/dataextractor/container/http"
	"log"
	"os"
	"path"
)

const (
	DefaultLogFilePath    = "logs/sde.log"
	DefaultConfigFilePath = "etc/sde.conf"
)

type DataExtractorMain struct {
	config        *Config
	buildInfo     *common.BuildInfo
	runtimeInfo   *common.RuntimeInfo
	webServerTask *http.WebServerTask
	manager       *manager.PipelineManager
}

func DoMain() {
	dataExtractor, _ := newDataExtractor()
	dataExtractor.webServerTask.Run()
}

func newDataExtractor() (*DataExtractorMain, error) {
	loggerFile, _ := os.OpenFile(DefaultLogFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.SetOutput(loggerFile)

	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	exPath := path.Dir(ex)
	log.Println("Current Folder: ", exPath)

	config := NewConfig()
	config.FromTomlFile(DefaultConfigFilePath)

	hostName, _ := os.Hostname()
	var httpUrl = "http://" + hostName + config.Http.BindAddress

	buildInfo, _ := common.NewBuildInfo()
	runtimeInfo, _ := common.NewRuntimeInfo(httpUrl)
	pipelineManager, _ := manager.New()
	webServerTask, _ := http.NewWebServerTask(config.Http, buildInfo, pipelineManager)
	dpm.RegisterWithDPM(config.DPM, buildInfo, runtimeInfo)

	return &DataExtractorMain{
		config:        config,
		buildInfo:     buildInfo,
		runtimeInfo:   runtimeInfo,
		webServerTask: webServerTask,
		manager:       pipelineManager,
	}, nil
}

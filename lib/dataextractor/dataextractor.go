package dataextractor

import (
	"os"
	"log"
	"path"
	"github.com/streamsets/dataextractor/lib/http"
	"github.com/streamsets/dataextractor/lib/execution/manager"
	"github.com/streamsets/dataextractor/lib/common"
	"github.com/streamsets/dataextractor/lib/dpm"
)

const (
	DefaultLogFilePath = "logs/sde.log"
	DefaultConfigFilePath = "etc/sde.toml"
)

type DataExtractor struct {
	logger *log.Logger
	config *Config
	buildInfo *common.BuildInfo
	runtimeInfo *common.RuntimeInfo
	webServerTask *http.WebServerTask
	manager *manager.PipelineManager
}

func DoMain() {
	dataExtractor, _ := newDataExtractor()
	dataExtractor.webServerTask.Run()
}

func newDataExtractor() (*DataExtractor, error) {
	loggerFile, _ := os.OpenFile(DefaultLogFilePath,  os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	logger := log.New(loggerFile, "", log.Ldate|log.Ltime|log.Lshortfile)

	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	exPath := path.Dir(ex)
	logger.Println("Current Folder: ", exPath)

	config := NewConfig()
	config.FromTomlFile(DefaultConfigFilePath)

	hostName, _ := os.Hostname()
	var httpUrl = "http://" + hostName + config.Http.BindAddress

	buildInfo, _ := common.NewBuildInfo()
	runtimeInfo, _ := common.NewRuntimeInfo(logger, httpUrl)
	pipelineManager, _ := manager.New(logger)
	webServerTask, _ := http.NewWebServerTask(logger, config.Http, buildInfo, pipelineManager)
	dpm.RegisterWithDPM(config.DPM, buildInfo, runtimeInfo)

	return &DataExtractor{
		logger: logger,
		config: config,
		buildInfo: buildInfo,
		runtimeInfo: runtimeInfo,
		webServerTask: webServerTask,
		manager: pipelineManager,
	}, nil
}

package dataextractor

import (
	"flag"
	"github.com/streamsets/dataextractor/container/common"
	"github.com/streamsets/dataextractor/container/dpm"
	"github.com/streamsets/dataextractor/container/execution/manager"
	"github.com/streamsets/dataextractor/container/http"
	"github.com/streamsets/dataextractor/container/util"
	"log"
	"os"
	"path"
)

const (
	DefaultLogFilePath    = "logs/sde.log"
	DefaultConfigFilePath = "etc/sde.conf"
	DEBUG                 = "DEBUG"
	WARN                  = "WARN"
	ERROR                 = "ERROR"
	INFO                  = "INFO"
)

type DataExtractorMain struct {
	config        *Config
	buildInfo     *common.BuildInfo
	runtimeInfo   *common.RuntimeInfo
	webServerTask *http.WebServerTask
	manager       *manager.PipelineManager
}

func DoMain() {
	debugFlag := flag.Bool("debug", false, "Debug flag")
	flag.Parse()
	initializeLog(*debugFlag)
	dataExtractor, _ := newDataExtractor()
	dataExtractor.webServerTask.Run()
}

func newDataExtractor() (*DataExtractorMain, error) {
	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	exPath := path.Dir(ex)
	log.Println("[INFO] Current Folder: ", exPath)

	config := NewConfig()
	config.FromTomlFile(DefaultConfigFilePath)

	hostName, _ := os.Hostname()
	var httpUrl = "http://" + hostName + config.Http.BindAddress

	buildInfo, _ := common.NewBuildInfo()
	runtimeInfo, _ := common.NewRuntimeInfo(httpUrl)
	pipelineManager, _ := manager.New(config.Execution)
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

func initializeLog(debugFlag bool) {
	minLevel := util.LogLevel(WARN)
	if debugFlag {
		minLevel = util.LogLevel(DEBUG)
	}

	loggerFile, _ := os.OpenFile(DefaultLogFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	logFilter := &util.LevelFilter{
		Levels:   []util.LogLevel{DEBUG, WARN, ERROR, INFO},
		MinLevel: minLevel,
		Writer:   loggerFile,
	}
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.SetOutput(logFilter)

	log.Print("[DEBUG] Debugging")         // this will not print
	log.Print("[WARN] Warning")            // this will
	log.Print("[ERROR] Erring")            // and so will this
	log.Print("Message I haven't updated") // and so will this
}

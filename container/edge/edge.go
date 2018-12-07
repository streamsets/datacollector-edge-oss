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
package edge

import (
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/controlhub"
	"github.com/streamsets/datacollector-edge/container/execution/manager"
	"github.com/streamsets/datacollector-edge/container/http"
	"github.com/streamsets/datacollector-edge/container/process"
	"github.com/streamsets/datacollector-edge/container/store"
	"os"
	"path"
	"runtime"
	"strings"
)

const (
	DefaultLogFilePath    = "/log/edge.log"
	DefaultConfigFilePath = "/etc/edge.conf"
	LogFileName           = "edge.log"
	DEBUG                 = "DEBUG"
	WARN                  = "WARN"
	ERROR                 = "ERROR"
	INFO                  = "INFO"
)

type DataCollectorEdgeMain struct {
	Config                 *Config
	BuildInfo              *common.BuildInfo
	RuntimeInfo            *common.RuntimeInfo
	WebServerTask          *http.WebServerTask
	PipelineStoreTask      store.PipelineStoreTask
	Manager                manager.Manager
	processManager         process.Manager
	DPMMessageEventHandler *controlhub.MessageEventHandler
}

func DoMain(
	baseDir string,
	debugFlag bool,
	logToConsoleFlag bool,
	startFlag string,
	runtimeParametersArg string,
) (*DataCollectorEdgeMain, error) {
	dataCollectorEdge, err := newDataCollectorEdge(baseDir, debugFlag, logToConsoleFlag)
	if err != nil {
		panic(err)
	}

	if len(startFlag) > 0 {
		var runtimeParameters map[string]interface{}
		if len(runtimeParametersArg) > 0 {
			err := json.Unmarshal([]byte(runtimeParametersArg), &runtimeParameters)
			if err != nil {
				panic(err)
			}
		}

		fmt.Println("Starting Pipeline: ", startFlag)
		state, err := dataCollectorEdge.Manager.GetRunner(startFlag).GetStatus()
		if state != nil && state.Status == common.RUNNING {
			// If status is running, change it back to stopped
			dataCollectorEdge.Manager.StopPipeline(startFlag)
		}

		state, err = dataCollectorEdge.Manager.StartPipeline(startFlag, runtimeParameters)
		if err != nil {
			fmt.Println(err)
			log.Panic(err)
		}
		stateJson, _ := json.Marshal(state)
		fmt.Println(string(stateJson))
	}

	return dataCollectorEdge, nil
}

func newDataCollectorEdge(
	baseDir string,
	debugFlag bool,
	logToConsoleFlag bool,
) (*DataCollectorEdgeMain, error) {
	config := NewConfig()
	err := config.FromTomlFile(baseDir + DefaultConfigFilePath)
	if err != nil {
		return nil, err
	}

	err = initializeLog(debugFlag, logToConsoleFlag, baseDir, config.LogDir)
	if err != nil {
		return nil, err
	}

	hostName, _ := os.Hostname()
	var httpUrl = "http://" + hostName + config.Http.BindAddress

	if len(config.Http.BaseHttpUrl) > 0 {
		httpUrl = config.Http.BaseHttpUrl
	}

	buildInfo, _ := common.NewBuildInfo()
	fmt.Println("Version: ", buildInfo.Version)
	fmt.Println("Base Dir: ", baseDir)

	if !logToConsoleFlag {
		log.WithField("version", buildInfo.Version).Info()
		log.WithField("baseDir", baseDir).Info()
	}

	runtimeInfo, _ := common.NewRuntimeInfo(httpUrl, baseDir)
	pipelineStoreTask := store.NewFilePipelineStoreTask(*runtimeInfo)
	pipelineManager, _ := manager.NewManager(config.Execution, runtimeInfo, pipelineStoreTask)

	processManager, err := process.NewManager(config.Process)

	if err != nil {
		return nil, err
	}

	webServerTask, _ := http.NewWebServerTask(config.Http, buildInfo, pipelineManager, pipelineStoreTask, processManager)
	controlhub.RegisterWithControlHub(config.SCH, buildInfo, runtimeInfo)

	var messagingEventHandler *controlhub.MessageEventHandler
	if runtimeInfo.DPMEnabled {
		messagingEventHandler = controlhub.NewMessageEventHandler(
			config.SCH,
			buildInfo, runtimeInfo,
			pipelineStoreTask,
			pipelineManager,
		)
		messagingEventHandler.Init()
	}

	return &DataCollectorEdgeMain{
		Config:                 config,
		BuildInfo:              buildInfo,
		RuntimeInfo:            runtimeInfo,
		WebServerTask:          webServerTask,
		Manager:                pipelineManager,
		PipelineStoreTask:      pipelineStoreTask,
		DPMMessageEventHandler: messagingEventHandler,
	}, nil
}

type ContextHook struct{}

func (hook ContextHook) Levels() []log.Level {
	return log.AllLevels
}

/* Adds file/line info to logrus fields */
func (hook ContextHook) Fire(entry *log.Entry) error {
	pc := make([]uintptr, 3, 3)
	cnt := runtime.Callers(6, pc)

	for i := 0; i < cnt; i++ {
		fu := runtime.FuncForPC(pc[i] - 1)
		name := fu.Name()
		if !strings.Contains(name, "github.com/Sirupsen/logrus") {
			file, line := fu.FileLine(pc[i] - 1)
			entry.Data["file"] = fmt.Sprintf("%s:%d", path.Base(file), line)
			break
		}
	}
	return nil
}

func initializeLog(debugFlag bool, logToConsoleFlag bool, baseDir string, logDirArg string) error {
	minLevel := log.InfoLevel
	if debugFlag {
		minLevel = log.DebugLevel
		log.AddHook(ContextHook{})
	}

	if logToConsoleFlag && logDirArg != "" {
		panic(errors.New("logDir argument is supported only when writing logs to file"))
	}

	var loggerFile *os.File
	var err error

	if logToConsoleFlag {
		loggerFile = os.Stdout
	} else {
		logFile := baseDir + DefaultLogFilePath
		if logDirArg != "" {
			logFile = logDirArg + "/" + LogFileName
		}
		loggerFile, err = os.OpenFile(logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return err
		}
	}

	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})
	log.SetLevel(minLevel)
	log.SetOutput(loggerFile)

	return nil
}

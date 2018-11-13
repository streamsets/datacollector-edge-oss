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
package http

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/julienschmidt/httprouter"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/execution/manager"
	"github.com/streamsets/datacollector-edge/container/process"
	"github.com/streamsets/datacollector-edge/container/store"
	"github.com/streamsets/datacollector-edge/container/util"
	"net/http"
	"net/http/pprof"
	"os"
)

const (
	ContentType     = "Content-Type"
	ApplicationJson = "application/json"
)

type WebServerTask struct {
	config            Config
	buildInfo         *common.BuildInfo
	manager           manager.Manager
	pipelineStoreTask store.PipelineStoreTask
	httpServer        *http.Server
	processManager    *process.Manager
}

func (webServerTask *WebServerTask) Init() error {
	if !webServerTask.config.Enabled {
		fmt.Println("Web Server is disabled")
		log.Info("Web Server is disabled")
		return nil
	}

	fmt.Println("Running on URI : http://localhost" + webServerTask.config.BindAddress)
	log.Info("Running on URI : http://localhost" + webServerTask.config.BindAddress)

	router := httprouter.New()
	router.GET("/", webServerTask.homeHandler)

	// Manager APIs
	router.POST("/rest/v1/pipeline/:pipelineId/start", webServerTask.startHandler)
	router.POST("/rest/v1/pipeline/:pipelineId/stop", webServerTask.stopHandler)
	router.POST("/rest/v1/pipeline/:pipelineId/resetOffset", webServerTask.resetOffsetHandler)
	router.POST("/rest/v1/pipeline/:pipelineId/committedOffsets", webServerTask.updateOffsetHandler)

	router.GET("/rest/v1/pipeline/:pipelineId/status", webServerTask.statusHandler)
	router.GET("/rest/v1/pipeline/:pipelineId/history", webServerTask.historyHandler)
	router.GET("/rest/v1/pipeline/:pipelineId/metrics", webServerTask.metricsHandler)
	router.GET("/rest/v1/pipeline/:pipelineId/committedOffsets", webServerTask.getOffsetHandler)
	router.GET("/rest/v1/pipeline/:pipelineId/errorRecords", webServerTask.getErrorRecords)
	router.GET("/rest/v1/pipeline/:pipelineId/errorMessages", webServerTask.getErrorMessages)

	// Pipeline Store APIs
	router.GET("/rest/v1/pipelines", webServerTask.getPipelines)
	router.GET("/rest/v1/pipeline/:pipelineId", webServerTask.getPipeline)
	router.PUT("/rest/v1/pipeline/:pipelineTitle", webServerTask.createPipeline)
	router.POST("/rest/v1/pipeline/:pipelineId", webServerTask.savePipeline)

	// Pipeline Preview APIs
	router.GET("/rest/v1/pipeline/:pipelineId/validate", webServerTask.validateConfigs)
	router.POST("/rest/v1/pipeline/:pipelineId/preview", webServerTask.preview)
	router.GET("/rest/v1/pipeline/:pipelineId/preview/:previewerId/status", webServerTask.getPreviewStatus)
	router.GET("/rest/v1/pipeline/:pipelineId/preview/:previewerId", webServerTask.getPreviewData)

	// Register pprof handlers
	router.HandlerFunc("GET", "/debug/pprof/", pprof.Index)
	router.Handler("GET", "/debug/pprof/heap", pprof.Handler("heap"))
	router.Handler("GET", "/debug/pprof/goroutine", pprof.Handler("goroutine"))
	router.Handler("GET", "/debug/pprof/block", pprof.Handler("block"))
	router.HandlerFunc("GET", "/debug/pprof/cmdline", pprof.Cmdline)
	router.HandlerFunc("GET", "/debug/pprof/profile", pprof.Profile)
	router.HandlerFunc("GET", "/debug/pprof/symbol", pprof.Symbol)
	router.HandlerFunc("GET", "/debug/pprof/trace", pprof.Trace)

	router.GET("/rest/v1/processMetrics", webServerTask.processMetricsHandler)

	webServerTask.httpServer = &http.Server{Addr: webServerTask.config.BindAddress, Handler: router}
	return nil
}

func (webServerTask *WebServerTask) homeHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "\t")
	encoder.Encode(webServerTask.buildInfo)
}

func (webServerTask *WebServerTask) Run() {
	if webServerTask.config.Enabled {
		fmt.Println(webServerTask.httpServer.ListenAndServe())
	} else {
		// Block forever to run Edge process in background
		select {}
	}
}

func (webServerTask *WebServerTask) Shutdown() {
	if webServerTask.config.Enabled {
		err := webServerTask.httpServer.Shutdown(context.Background())
		if err != nil {
			log.WithError(err).Error("Error happened when shutting down web server")
		}
	} else {
		os.Exit(0)
	}
}

func (webServerTask *WebServerTask) processMetricsHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "\t")
	encoder.Encode(util.FormatMetricsRegistry(webServerTask.processManager.GetProcessMetrics()))
}

func serverErrorReq(w http.ResponseWriter, err string) {
	w.Header().Set(ContentType, ApplicationJson)
	w.WriteHeader(http.StatusInternalServerError)
	fmt.Fprintf(w, `{"result":"", "error":%q}`, err)
}

func NewWebServerTask(
	config Config,
	buildInfo *common.BuildInfo,
	manager manager.Manager,
	pipelineStoreTask store.PipelineStoreTask,
	processManager *process.Manager,
) (*WebServerTask, error) {
	webServerTask := WebServerTask{
		config:            config,
		buildInfo:         buildInfo,
		manager:           manager,
		pipelineStoreTask: pipelineStoreTask,
		processManager:    processManager,
	}
	err := webServerTask.Init()
	if err != nil {
		return nil, err
	}
	return &webServerTask, nil
}

package http

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/execution/manager"
	"github.com/streamsets/datacollector-edge/container/store"
	"log"
	"net/http"
)

type WebServerTask struct {
	config            Config
	buildInfo         *common.BuildInfo
	manager           *manager.PipelineManager
	pipelineStoreTask store.PipelineStoreTask
	httpServer        *http.Server
}

func (webServerTask *WebServerTask) Init() error {
	fmt.Println("Running on URI : http://localhost" + webServerTask.config.BindAddress)
	log.Println("[INFO] Running on URI : http://localhost" + webServerTask.config.BindAddress)
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

	// Pipeline Store APIs
	router.GET("/rest/v1/pipelines", webServerTask.getPipelines)
	router.GET("/rest/v1/pipeline/:pipelineId", webServerTask.getPipeline)
	router.PUT("/rest/v1/pipeline/:pipelineTitle", webServerTask.createPipeline)
	router.POST("/rest/v1/pipeline/:pipelineId", webServerTask.savePipeline)
	webServerTask.httpServer = &http.Server{Addr: webServerTask.config.BindAddress, Handler: router}
	return nil
}

func (webServerTask *WebServerTask) homeHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "\t")
	encoder.Encode(webServerTask.buildInfo)

}

func (webServerTask *WebServerTask) Run() {
	fmt.Println(webServerTask.httpServer.ListenAndServe())
}

func (webServerTask *WebServerTask) Shutdown() {
	err := webServerTask.httpServer.Shutdown(context.Background())
	if err != nil {
		log.Printf("[ERROR] Error happened when shutting webserver : %s\n", err.Error())
	}
}

func NewWebServerTask(
	config Config,
	buildInfo *common.BuildInfo,
	manager *manager.PipelineManager,
	pipelineStoreTask store.PipelineStoreTask,
) (*WebServerTask, error) {
	webServerTask := WebServerTask{
		config:            config,
		buildInfo:         buildInfo,
		manager:           manager,
		pipelineStoreTask: pipelineStoreTask,
	}
	err := webServerTask.Init()
	if err != nil {
		return nil, err
	}
	return &webServerTask, nil
}

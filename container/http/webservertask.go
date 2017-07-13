package http

import (
	"encoding/json"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/streamsets/sdc2go/container/common"
	"github.com/streamsets/sdc2go/container/execution/manager"
	"github.com/streamsets/sdc2go/container/store"
	"log"
	"net/http"
)

type WebServerTask struct {
	config            Config
	buildInfo         *common.BuildInfo
	manager           *manager.PipelineManager
	pipelineStoreTask store.PipelineStoreTask
}

func (webServerTask *WebServerTask) Init() error {
	return nil
}

func (webServerTask *WebServerTask) homeHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "\t")
	encoder.Encode(webServerTask.buildInfo)

}

func (webServerTask *WebServerTask) Run() {
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

	fmt.Println(http.ListenAndServe(webServerTask.config.BindAddress, router))
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

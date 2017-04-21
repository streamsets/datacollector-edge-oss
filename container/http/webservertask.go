package http

import (
	"encoding/json"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/streamsets/dataextractor/container/common"
	"github.com/streamsets/dataextractor/container/execution/manager"
	"log"
	"net/http"
)

type WebServerTask struct {
	config    Config
	buildInfo *common.BuildInfo
	manager   *manager.PipelineManager
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
	log.Println("Running on URI : http://localhost" + webServerTask.config.BindAddress)
	router := httprouter.New()
	router.GET("/", webServerTask.homeHandler)
	router.POST("/rest/v1/pipeline/:pipelineId/start", webServerTask.startHandler)
	router.POST("/rest/v1/pipeline/:pipelineId/stop", webServerTask.stopHandler)
	router.POST("/rest/v1/pipeline/:pipelineId/resetOffset", webServerTask.resetOffsetHandler)
	router.GET("/rest/v1/pipeline/:pipelineId/status", webServerTask.statusHandler)
	fmt.Println(http.ListenAndServe(webServerTask.config.BindAddress, router))
}

func NewWebServerTask(
	config Config,
	buildInfo *common.BuildInfo,
	manager *manager.PipelineManager,
) (*WebServerTask, error) {
	webServerTask := WebServerTask{config: config, buildInfo: buildInfo, manager: manager}
	err := webServerTask.Init()
	if err != nil {
		return nil, err
	}
	return &webServerTask, nil
}

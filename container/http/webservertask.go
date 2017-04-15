package http

import (
	"encoding/json"
	"fmt"
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

func (webServerTask *WebServerTask) homeHandler(w http.ResponseWriter, r *http.Request) {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "\t")
	encoder.Encode(webServerTask.buildInfo)

}

func (webServerTask *WebServerTask) Run() {
	fmt.Println("Running on URI : http://localhost" + webServerTask.config.BindAddress)
	log.Println("Running on URI : http://localhost" + webServerTask.config.BindAddress)
	http.HandleFunc("/", webServerTask.homeHandler)
	http.HandleFunc("/rest/v1/pipeline/start", webServerTask.startHandler)
	http.HandleFunc("/rest/v1/pipeline/stop", webServerTask.stopHandler)
	http.HandleFunc("/rest/v1/pipeline/resetOffset", webServerTask.resetOffsetHandler)
	http.HandleFunc("/rest/v1/pipeline/status", webServerTask.statusHandler)
	fmt.Println(http.ListenAndServe(webServerTask.config.BindAddress, nil))
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

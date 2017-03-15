package http

import (
	"net/http"
	"fmt"
	"log"
	"github.com/streamsets/dataextractor/lib/execution/manager"
	"encoding/json"
	"github.com/streamsets/dataextractor/lib/common"
)

type WebServerTask struct {
	logger *log.Logger
	config Config
	buildInfo *common.BuildInfo
	manager *manager.PipelineManager
}

func (webServerTask *WebServerTask) Init() error {
	return nil
}

func (webServerTask *WebServerTask) homeHandler(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(webServerTask.buildInfo)
}

func (webServerTask *WebServerTask) startHandler(w http.ResponseWriter, r *http.Request) {
	if (r.Method == "POST") {
		go webServerTask.manager.GetRunner().StartPipeline()
		fmt.Fprint(w, "Data Extractor started successfully")
	} else {
		fmt.Fprintf(w, "Method %s! is not supported", r.Method)
	}
}

func (webServerTask *WebServerTask) stopHandler(w http.ResponseWriter, r *http.Request) {
	if (r.Method == "POST") {
		go webServerTask.manager.GetRunner().StopPipeline()
		fmt.Fprint(w, "Data Extractor stopped successfully")
	} else {
		fmt.Fprintf(w, "Method %s! is not supported", r.Method)
	}
}

func (webServerTask *WebServerTask) resetOffsetHandler(w http.ResponseWriter, r *http.Request) {
	if (r.Method == "POST") {
		go webServerTask.manager.GetRunner().ResetOffset()
		fmt.Fprint(w, "Reset Origin is successful.")
	} else {
		fmt.Fprintf(w, "Method %s! is not supported", r.Method)
	}
}

func (webServerTask *WebServerTask) Run() {
	fmt.Println("Running on URI : http://localhost" + webServerTask.config.BindAddress)
	http.HandleFunc("/", webServerTask.homeHandler)
	http.HandleFunc("/rest/v1/pipeline/start", webServerTask.startHandler)
	http.HandleFunc("/rest/v1/pipeline/stop", webServerTask.stopHandler)
	http.HandleFunc("/rest/v1/pipeline/resetOffset", webServerTask.resetOffsetHandler)
	fmt.Println(http.ListenAndServe(webServerTask.config.BindAddress, nil))
}

func NewWebServerTask(
	logger *log.Logger,
	config Config,
	buildInfo *common.BuildInfo,
	manager *manager.PipelineManager,
)(*WebServerTask, error) {
	webServerTask := WebServerTask{logger: logger, config: config, buildInfo: buildInfo, manager: manager}
	err := webServerTask.Init()
	if err != nil {
		return nil, err
	}
	return &webServerTask, nil
}


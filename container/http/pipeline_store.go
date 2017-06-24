package http

import (
	"encoding/json"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/streamsets/sdc2go/container/common"
	"io"
	"net/http"
)

// Path - GET /rest/v1/pipelines
func (webServerTask *WebServerTask) getPipelines(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	pipelineInfoList, err := webServerTask.pipelineStoreTask.GetPipelines()
	if err == nil {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "\t")
		encoder.Encode(pipelineInfoList)
	} else {
		fmt.Fprintf(w, "Failed to get pipelines:  %s! ", err)
	}
}

// Path - GET /rest/v1/pipeline/:pipelineId
func (webServerTask *WebServerTask) getPipeline(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	pipelineId := ps.ByName("pipelineId")
	pipelineConfig, err := webServerTask.pipelineStoreTask.LoadPipelineConfig(pipelineId)
	if err == nil {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "\t")
		encoder.Encode(pipelineConfig)
	} else {
		fmt.Fprintf(w, "Failed to get pipeline:  %s! ", err)
	}
}

// Path - PUT /rest/v1/pipeline/:pipelineId?description=<desc>
func (webServerTask *WebServerTask) createPipeline(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	pipelineTitle := ps.ByName("pipelineTitle")
	description := r.URL.Query().Get("description")
	pipelineConfig, err := webServerTask.pipelineStoreTask.Create(pipelineTitle, pipelineTitle, description)
	if err == nil {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "\t")
		encoder.Encode(pipelineConfig)
	} else {
		fmt.Fprintf(w, "Failed to create pipeline:  %s! ", err)
	}
}

// Path - POST /rest/v1/pipeline/:pipelineId
func (webServerTask *WebServerTask) savePipeline(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	pipelineId := ps.ByName("pipelineId")

	decoder := json.NewDecoder(r.Body)
	var pipelineConfiguration common.PipelineConfiguration
	err := decoder.Decode(&pipelineConfiguration)
	if err != nil {
		switch {
		case err == io.EOF:
			// empty body
		case err != nil:
			// other error
			fmt.Fprintf(w, "Failed to Start: %s", err)
			return
		}
	}
	defer r.Body.Close()

	pipelineConfig, err := webServerTask.pipelineStoreTask.Save(pipelineId, pipelineConfiguration)
	if err == nil {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "\t")
		encoder.Encode(pipelineConfig)
	} else {
		fmt.Fprintf(w, "Failed to create pipeline:  %s! ", err)
	}
}

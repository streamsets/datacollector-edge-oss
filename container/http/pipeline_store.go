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
	"encoding/json"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/streamsets/datacollector-edge/container/common"
	"io"
	"net/http"
)

// Path - GET /rest/v1/pipelines
func (webServerTask *WebServerTask) getPipelines(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set(ContentType, ApplicationJson)
	pipelineInfoList, err := webServerTask.pipelineStoreTask.GetPipelines()
	if err == nil {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "\t")
		encoder.Encode(pipelineInfoList)
	} else {
		serverErrorReq(w, fmt.Sprintf("Failed to get pipelines:  %s! ", err))
	}
}

// Path - GET /rest/v1/pipeline/:pipelineId
func (webServerTask *WebServerTask) getPipeline(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set(ContentType, ApplicationJson)
	pipelineId := ps.ByName("pipelineId")
	pipelineConfig, err := webServerTask.pipelineStoreTask.LoadPipelineConfig(pipelineId)
	if err == nil {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "\t")
		encoder.Encode(pipelineConfig)
	} else {
		serverErrorReq(w, fmt.Sprintf("Failed to get pipeline:  %s! ", err))
	}
}

// Path - PUT /rest/v1/pipeline/:pipelineId?description=<desc>
func (webServerTask *WebServerTask) createPipeline(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set(ContentType, ApplicationJson)
	pipelineTitle := ps.ByName("pipelineTitle")
	description := r.URL.Query().Get("description")
	pipelineConfig, err := webServerTask.pipelineStoreTask.Create(pipelineTitle, pipelineTitle, description, false)
	if err == nil {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "\t")
		encoder.Encode(pipelineConfig)
	} else {
		serverErrorReq(w, fmt.Sprintf("Failed to create pipeline:  %s! ", err))
	}
}

// Path - POST /rest/v1/pipeline/:pipelineId
func (webServerTask *WebServerTask) savePipeline(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set(ContentType, ApplicationJson)
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
			serverErrorReq(w, fmt.Sprintf("Failed to save pipeline:  %s! ", err))
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
		serverErrorReq(w, fmt.Sprintf("Failed to save pipeline:  %s! ", err))
	}
}

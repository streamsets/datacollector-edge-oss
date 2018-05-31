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
	"github.com/julienschmidt/httprouter"
	"github.com/streamsets/datacollector-edge/container/execution"
	"io"
	"net/http"
	"strconv"
)

type PreviewInfo struct {
	PreviewerID string `json:"previewerId"`
	Status      string `json:"status"`
}

// Path - POST /rest/v1/pipeline/{pipelineId}/preview
func (webServerTask *WebServerTask) preview(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set(ContentType, ApplicationJson)

	batches := 1
	batchSize := 10
	skipTargets := true
	timeout := int64(30000)
	testOrigin := false

	pipelineId := ps.ByName("pipelineId")
	if i, err := strconv.Atoi(r.URL.Query().Get("batches")); err == nil {
		batches = i
	}
	if i, err := strconv.Atoi(r.URL.Query().Get("batchSize")); err == nil {
		batchSize = i
	}
	if b, err := strconv.ParseBool(r.URL.Query().Get("skipTargets")); err == nil {
		skipTargets = b
	}
	if i, err := strconv.ParseInt(r.URL.Query().Get("timeout"), 10, 64); err == nil {
		timeout = i
	}
	if b, err := strconv.ParseBool(r.URL.Query().Get("testOrigin")); err == nil {
		testOrigin = b
	}
	endStage := r.URL.Query().Get("endStage")

	decoder := json.NewDecoder(r.Body)
	var stageOutputsToOverride []execution.StageOutputJson
	err := decoder.Decode(&stageOutputsToOverride)
	if err != nil {
		switch {
		case err == io.EOF:
			// empty body
		case err != nil:
			// other error
			serverErrorReq(w, err.Error())
			return
		}
	}
	defer r.Body.Close()

	previewer, err := webServerTask.manager.CreatePreviewer(pipelineId)
	if err != nil {
		serverErrorReq(w, err.Error())
		return
	}

	err = previewer.Start(batches, batchSize, skipTargets, endStage, stageOutputsToOverride, timeout, testOrigin)
	if err != nil {
		serverErrorReq(w, err.Error())
		return
	}

	previewInfo := PreviewInfo{
		PreviewerID: previewer.GetId(),
		Status:      previewer.GetStatus(),
	}

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "\t")
	encoder.Encode(previewInfo)
}

// Path - GET /rest/v1/pipeline/{pipelineId}/preview/{previewerId}/status
func (webServerTask *WebServerTask) getPreviewStatus(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set(ContentType, ApplicationJson)

	previewerId := ps.ByName("previewerId")
	previewer, err := webServerTask.manager.GetPreviewer(previewerId)
	if err != nil {
		serverErrorReq(w, err.Error())
		return
	}

	previewInfo := PreviewInfo{
		PreviewerID: previewer.GetId(),
		Status:      previewer.GetStatus(),
	}

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "\t")
	encoder.Encode(previewInfo)
}

// Path - GET /rest/v1/pipeline/{pipelineId}/preview/{previewerId}
func (webServerTask *WebServerTask) getPreviewData(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set(ContentType, ApplicationJson)

	previewerId := ps.ByName("previewerId")
	previewer, err := webServerTask.manager.GetPreviewer(previewerId)
	if err != nil {
		serverErrorReq(w, err.Error())
	}

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "\t")
	encoder.Encode(previewer.GetOutput())
}

// Path - DELETE /rest/v1/pipeline/{pipelineId}/preview/{previewerId}
func (webServerTask *WebServerTask) stopPreview(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set(ContentType, ApplicationJson)

	previewerId := ps.ByName("previewerId")
	previewer, err := webServerTask.manager.GetPreviewer(previewerId)
	if err != nil {
		serverErrorReq(w, err.Error())
	}

	err = previewer.Stop()
	if err != nil {
		serverErrorReq(w, err.Error())
	}

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "\t")
	encoder.Encode(previewer.GetOutput())
}

// Path - GET /rest/v1/pipeline/{pipelineId}/validate
func (webServerTask *WebServerTask) validateConfigs(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set(ContentType, ApplicationJson)

	timeout := int64(30000)

	pipelineId := ps.ByName("pipelineId")
	if i, err := strconv.ParseInt(r.URL.Query().Get("timeout"), 10, 64); err == nil {
		timeout = i
	}

	previewer, err := webServerTask.manager.CreatePreviewer(pipelineId)
	if err != nil {
		serverErrorReq(w, err.Error())
		return
	}

	err = previewer.ValidateConfigs(timeout)
	if err != nil {
		serverErrorReq(w, err.Error())
		return
	}

	previewInfo := PreviewInfo{
		PreviewerID: previewer.GetId(),
		Status:      previewer.GetStatus(),
	}

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "\t")
	encoder.Encode(previewInfo)
}

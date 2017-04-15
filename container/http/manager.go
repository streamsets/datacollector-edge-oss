package http

import (
	"fmt"
	"encoding/json"
	"net/http"
)

func (webServerTask *WebServerTask) startHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		pipelineId := r.URL.Query().Get("pipelineId")
		state, err := webServerTask.manager.StartPipeline(pipelineId)
		if err == nil {
			encoder := json.NewEncoder(w)
			encoder.SetIndent("", "\t")
			encoder.Encode(state)
		} else {
			fmt.Fprintf(w, "Failed to Start:  %s! ", err)
		}

	} else {
		fmt.Fprintf(w, "Method %s! is not supported", r.Method)
	}
}

func (webServerTask *WebServerTask) stopHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		pipelineId := r.URL.Query().Get("pipelineId")
		state, err := webServerTask.manager.StopPipeline(pipelineId)
		if err == nil {
			encoder := json.NewEncoder(w)
			encoder.SetIndent("", "\t")
			encoder.Encode(state)
		} else {
			fmt.Fprintf(w, "Failed to Stop:  %s! ", err)
		}
	} else {
		fmt.Fprintf(w, "Method %s! is not supported", r.Method)
	}
}

func (webServerTask *WebServerTask) resetOffsetHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		pipelineId := r.URL.Query().Get("pipelineId")
		go webServerTask.manager.ResetOffset(pipelineId)
		fmt.Fprint(w, "Reset Origin is successful.")
	} else {
		fmt.Fprintf(w, "Method %s! is not supported", r.Method)
	}
}

func (webServerTask *WebServerTask) statusHandler(w http.ResponseWriter, r *http.Request) {
	pipelineId := r.URL.Query().Get("pipelineId")
	state, err := webServerTask.manager.GetRunner(pipelineId).GetStatus()
	if err == nil {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "\t")
		encoder.Encode(state)
	} else {
		fmt.Fprintf(w, "Failed to get status:  %s! ", err)
	}
}
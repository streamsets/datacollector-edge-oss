package http

import (
	"encoding/json"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"net/http"
)

func (webServerTask *WebServerTask) startHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	pipelineId := ps.ByName("pipelineId")
	state, err := webServerTask.manager.StartPipeline(pipelineId)
	if err == nil {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "\t")
		encoder.Encode(state)
	} else {
		fmt.Fprintf(w, "Failed to Start:  %s! ", err)
	}

}

func (webServerTask *WebServerTask) stopHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	pipelineId := ps.ByName("pipelineId")
	state, err := webServerTask.manager.StopPipeline(pipelineId)
	if err == nil {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "\t")
		encoder.Encode(state)
	} else {
		fmt.Fprintf(w, "Failed to Stop:  %s! ", err)
	}
}

func (webServerTask *WebServerTask) resetOffsetHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	pipelineId := ps.ByName("pipelineId")
	err := webServerTask.manager.ResetOffset(pipelineId)
	if err == nil {
		fmt.Fprint(w, "Reset Origin failed:  %s! ", err)
	} else {
		fmt.Fprint(w, "Reset Origin is successful.")
	}
}

func (webServerTask *WebServerTask) statusHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	pipelineId := ps.ByName("pipelineId")
	state, err := webServerTask.manager.GetRunner(pipelineId).GetStatus()
	if err == nil {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "\t")
		encoder.Encode(state)
	} else {
		fmt.Fprintf(w, "Failed to get status:  %s! ", err)
	}
}

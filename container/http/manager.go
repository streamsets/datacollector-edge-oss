package http

import (
	"encoding/json"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/streamsets/sdc2go/container/common"
	"github.com/streamsets/sdc2go/container/util"
	"io"
	"net/http"
)

func (webServerTask *WebServerTask) startHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	pipelineId := ps.ByName("pipelineId")
	decoder := json.NewDecoder(r.Body)
	var runtimeParameters map[string]interface{}
	err := decoder.Decode(&runtimeParameters)
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

	state, err := webServerTask.manager.StartPipeline(pipelineId, runtimeParameters)
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
		fmt.Fprint(w, "Reset Origin is successful.")
	} else {
		fmt.Fprint(w, "Reset Origin failed: ", err)
	}
}

func (webServerTask *WebServerTask) getOffsetHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	pipelineId := ps.ByName("pipelineId")
	sourceOffset, err := webServerTask.manager.GetRunner(pipelineId).GetOffset()
	if err == nil {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "\t")
		encoder.Encode(sourceOffset)
	} else {
		fmt.Fprintf(w, "Failed to get status:  %s! ", err)
	}
}

func (webServerTask *WebServerTask) updateOffsetHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	pipelineId := ps.ByName("pipelineId")
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()
	var sourceOffset common.SourceOffset
	err := decoder.Decode(&sourceOffset)
	if err == nil {
		err = webServerTask.manager.GetRunner(pipelineId).CommitOffset(sourceOffset)
	}
	if err != nil {
		if err == io.EOF {
			// empty body
			fmt.Fprint(w, "Failed to updateOffsets: %s", "Offset Data is missing in the request body")
		} else {
			// other error
			fmt.Fprintf(w, "Failed to updateOffsets: %s", err)
		}
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

func (webServerTask *WebServerTask) historyHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	pipelineId := ps.ByName("pipelineId")
	pipelineHistoryStates, err := webServerTask.manager.GetRunner(pipelineId).GetHistory()
	if err == nil {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "\t")
		encoder.Encode(pipelineHistoryStates)
	} else {
		fmt.Fprintf(w, "Failed to get history:  %s! ", err)
	}
}

func (webServerTask *WebServerTask) metricsHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	pipelineId := ps.ByName("pipelineId")
	metricRegistry, err := webServerTask.manager.GetRunner(pipelineId).GetMetrics()
	if err == nil {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "\t")
		encoder.Encode(util.FormatMetricsRegistry(metricRegistry))
	} else {
		fmt.Fprintf(w, "Failed to get metrics:  %s! ", err)
	}
}

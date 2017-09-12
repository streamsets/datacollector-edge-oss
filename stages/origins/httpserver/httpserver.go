package httpserver

import (
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"
)

const (
	LIBRARY    = "streamsets-datacollector-basic-lib"
	STAGE_NAME = "com_streamsets_pipeline_stage_origin_httpserver_HttpServerDPushSource"
)

type HttpServerOrigin struct {
	*common.BaseStage
	Port         float64 `ConfigDef:"name=httpConfigs.port,type=NUMBER,required=true"`
	AppId        string  `ConfigDef:"name=httpConfigs.appId,type=STRING,required=true"`
	httpServer   *http.Server
	incomingData chan interface{}
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &HttpServerOrigin{BaseStage: &common.BaseStage{}}
	})
}

func (h *HttpServerOrigin) Init(stageContext api.StageContext) error {
	if err := h.BaseStage.Init(stageContext); err != nil {
		return err
	}
	h.httpServer = h.startHttpServer()
	h.incomingData = make(chan interface{})
	return nil
}

func (h *HttpServerOrigin) Destroy() error {
	if err := h.httpServer.Shutdown(nil); err != nil {
		return err
	}
	log.Println("[DEBUG] HTTP Server - server shutdown successfully")
	return nil
}

func (h *HttpServerOrigin) Produce(
	lastSourceOffset string,
	maxBatchSize int,
	batchMaker api.BatchMaker,
) (string, error) {
	log.Println("[DEBUG] HTTP Server - Produce method")
	value := <-h.incomingData
	log.Println("[DEBUG] Incoming Data: ", value)
	record, _ := h.GetStageContext().CreateRecord(time.Now().String(), value)
	batchMaker.AddRecord(record)
	return "", nil
}

func (h *HttpServerOrigin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("[DEBUG] HTTP Server error reading request body : ", err)
		h.GetStageContext().ReportError(err)
	} else {
		h.incomingData <- string(body)
	}
}

func (h *HttpServerOrigin) startHttpServer() *http.Server {
	srv := &http.Server{
		Addr:    ":" + strconv.FormatFloat(h.Port, 'E', -1, 64),
		Handler: h,
	}

	go func() {
		log.Println("[DEBUG] HTTP Server - Running on URI : http://localhost:", h.Port)
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("[ERROR] Httpserver: ListenAndServe() error: %s", err)
			h.GetStageContext().ReportError(err)
		}
	}()

	return srv
}

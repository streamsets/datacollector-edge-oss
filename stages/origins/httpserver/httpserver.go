package httpserver

import (
	"context"
	"github.com/streamsets/dataextractor/api"
	"github.com/streamsets/dataextractor/container/common"
	"github.com/streamsets/dataextractor/stages/stagelibrary"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
)

const (
	LIBRARY    = "streamsets-datacollector-basic-lib"
	STAGE_NAME = "com_streamsets_pipeline_stage_origin_httpserver_HttpServerDPushSource"
)

type HttpServerOrigin struct {
	port         int64
	appId        string
	httpServer   *http.Server
	incomingData chan interface{}
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &HttpServerOrigin{}
	})
}

func (h *HttpServerOrigin) Init(ctx context.Context) error {
	stageContext := (ctx.Value("stageContext")).(common.StageContext)
	stageConfig := stageContext.StageConfig
	for _, config := range stageConfig.Configuration {
		if config.Name == "httpConfigs.port" {
			h.port = stageContext.GetResolvedValue(config.Value).(int64)
		}
		if config.Name == "httpConfigs.appId" {
			h.appId = stageContext.GetResolvedValue(config.Value).(string)
		}
	}

	h.httpServer = h.startHttpServer()
	h.incomingData = make(chan interface{})
	return nil
}

func (h *HttpServerOrigin) Destroy() error {
	if err := h.httpServer.Shutdown(nil); err != nil {
		panic(err)
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
	batchMaker.AddRecord(api.Record{Value: value})
	return "", nil
}

func (h *HttpServerOrigin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("[DEBUG] HTTP Server error reading request body : ", err)
	} else {
		h.incomingData <- string(body)
	}
}

func (h *HttpServerOrigin) startHttpServer() *http.Server {
	srv := &http.Server{
		Addr:    ":" + strconv.FormatInt(h.port, 10),
		Handler: h,
	}

	go func() {
		log.Println("[DEBUG] HTTP Server - Running on URI : http://localhost:", h.port)
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("[ERROR] Httpserver: ListenAndServe() error: %s", err)
		}
	}()

	return srv
}

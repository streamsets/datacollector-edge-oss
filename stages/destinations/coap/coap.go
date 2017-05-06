package coap

import (
	"context"
	"encoding/json"
	"github.com/dustin/go-coap"
	"github.com/streamsets/dataextractor/api"
	"github.com/streamsets/dataextractor/container/common"
	"github.com/streamsets/dataextractor/stages/stagelibrary"
	"log"
	"net/url"
)

const (
	LIBRARY    = "streamsets-datacollector-basic-lib"
	STAGE_NAME = "com_streamsets_pipeline_stage_destination_coap_CoapClientDTarget"
)

type CoapClientDestination struct {
	resourceUrl string
	coapMethod  string
	requestType string
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &CoapClientDestination{}
	})
}

func (c *CoapClientDestination) Init(ctx context.Context) {
	stageContext := (ctx.Value("stageContext")).(common.StageContext)
	stageConfig := stageContext.StageConfig
	log.Println("[DEBUG] MqttClientDestination Init method")
	for _, config := range stageConfig.Configuration {
		if config.Name == "conf.resourceUrl" {
			c.resourceUrl = config.Value.(string)
		}

		if config.Name == "conf.coapMethod" {
			c.coapMethod = config.Value.(string)
		}

		if config.Name == "conf.requestType" {
			c.requestType = config.Value.(string)
		}
	}
}

func (c *CoapClientDestination) Write(batch api.Batch) error {
	log.Println("[DEBUG] CoapClientDestination Write method")
	for _, record := range batch.GetRecords() {
		c.sendRecordToSDC(record.Value)
	}
	return nil
}

func (c *CoapClientDestination) sendRecordToSDC(recordValue interface{}) {
	jsonValue, err := json.Marshal(recordValue)
	if err != nil {
		panic(err)
	}

	parsedURL, err := url.Parse(c.resourceUrl)
	if err != nil {
		panic(err)
	}

	req := coap.Message{
		Type:    getCoapType(c.requestType),
		Code:    getCoapMethod(c.coapMethod),
		Payload: jsonValue,
	}
	req.SetPathString(parsedURL.Path)

	coapClient, err := coap.Dial("udp", parsedURL.Host)
	if err != nil {
		log.Printf("[ERROR] Error dialing: %v", err)
	}

	rv, err := coapClient.Send(req)
	if err != nil {
		log.Printf("[ERROR] Error sending request: %v", err)
	}

	if rv != nil {
		log.Printf("[DEBUG] Response payload: %s", rv.Payload)
	}
}

func (h *CoapClientDestination) Destroy() {
}

func getCoapType(requestType string) coap.COAPType {
	switch requestType {
	case "CONFIRMABLE":
		return coap.Confirmable
	case "NONCONFIRMABLE":
		return coap.NonConfirmable
	}
	return coap.NonConfirmable
}

func getCoapMethod(coapMethod string) coap.COAPCode {
	switch coapMethod {
	case "GET":
		return coap.GET
	case "POST":
		return coap.POST
	case "PUT":
		return coap.PUT
	case "DELETE":
		return coap.DELETE
	}
	return coap.POST
}

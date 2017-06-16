package coap

import (
	"encoding/json"
	"github.com/dustin/go-coap"
	"github.com/streamsets/sdc2go/api"
	"github.com/streamsets/sdc2go/container/common"
	"github.com/streamsets/sdc2go/stages/stagelibrary"
	"log"
	"net/url"
)

const (
	LIBRARY            = "streamsets-datacollector-basic-lib"
	STAGE_NAME         = "com_streamsets_pipeline_stage_destination_coap_CoapClientDTarget"
	CONF_RESOURCE_URL  = "conf.resourceUrl"
	CONF_COAP_METHOD   = "conf.coapMethod"
	CONF_RESOURCE_TYPE = "conf.requestType"
	CONFIRMABLE        = "CONFIRMABLE"
	NONCONFIRMABLE     = "NONCONFIRMABLE"
	GET                = "GET"
	POST               = "POST"
	PUT                = "PUT"
	DELETE             = "DELETE"
)

type CoapClientDestination struct {
	*common.BaseStage
	resourceUrl string
	coapMethod  string
	requestType string
}

var mid uint16

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &CoapClientDestination{BaseStage: &common.BaseStage{}}
	})
}

func (c *CoapClientDestination) Init(stageContext api.StageContext) error {
	if err := c.BaseStage.Init(stageContext); err != nil {
		return err
	}
	stageConfig := c.GetStageConfig()
	log.Println("[DEBUG] CoapClientDestination Init method")
	for _, config := range stageConfig.Configuration {
		if config.Name == CONF_RESOURCE_URL {
			c.resourceUrl = config.Value.(string)
		}

		if config.Name == CONF_COAP_METHOD {
			c.coapMethod = config.Value.(string)
		}

		if config.Name == CONF_RESOURCE_TYPE {
			c.requestType = config.Value.(string)
		}
	}

	mid = 0
	return nil
}

func (c *CoapClientDestination) Write(batch api.Batch) error {
	log.Println("[DEBUG] CoapClientDestination Write method")
	for _, record := range batch.GetRecords() {
		err := c.sendRecordToSDC(record.GetValue())
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *CoapClientDestination) sendRecordToSDC(recordValue interface{}) error {
	jsonValue, err := json.Marshal(recordValue)
	if err != nil {
		return err
	}

	parsedURL, err := url.Parse(c.resourceUrl)
	if err != nil {
		return err
	}

	req := coap.Message{
		Type:      getCoapType(c.requestType),
		Code:      getCoapMethod(c.coapMethod),
		MessageID: mid,
		Payload:   jsonValue,
	}
	req.SetPathString(parsedURL.Path)

	coapClient, err := coap.Dial("udp", parsedURL.Host)
	if err != nil {
		log.Printf("[ERROR] Error dialing: %v", err)
		return err
	}

	_, err = coapClient.Send(req)
	if err != nil {
		log.Printf("[ERROR] Error sending request: %v", err)
		return err
	}

	mid++
	return nil
}

func getCoapType(requestType string) coap.COAPType {
	switch requestType {
	case CONFIRMABLE:
		return coap.Confirmable
	case NONCONFIRMABLE:
		return coap.NonConfirmable
	}
	return coap.NonConfirmable
}

func getCoapMethod(coapMethod string) coap.COAPCode {
	switch coapMethod {
	case GET:
		return coap.GET
	case POST:
		return coap.POST
	case PUT:
		return coap.PUT
	case DELETE:
		return coap.DELETE
	}
	return coap.POST
}

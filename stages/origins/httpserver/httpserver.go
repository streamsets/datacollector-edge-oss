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
package httpserver

import (
	"bytes"
	"context"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/lib/dataparser"
	"github.com/streamsets/datacollector-edge/stages/lib/httpcommon"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"golang.org/x/crypto/pkcs12"
	"io/ioutil"
	"net/http"
	"strconv"
)

const (
	Library                        = "streamsets-datacollector-basic-lib"
	StageName                      = "com_streamsets_pipeline_stage_origin_httpserver_HttpServerDPushSource"
	X_SDC_APPLICATION_ID_HEADER    = "X-SDC-APPLICATION-ID"
	SDC_APPLICATION_ID_QUERY_PARAM = "sdcApplicationId"
	PKCS12                         = "PKCS12"
)

var stringOffset = "http-server-offset"

type Origin struct {
	*common.BaseStage
	HttpConfigs      RawHttpConfigs                    `ConfigDefBean:"name=httpConfigs"`
	DataFormat       string                            `ConfigDef:"type=STRING,required=true"`
	DataFormatConfig dataparser.DataParserFormatConfig `ConfigDefBean:"dataFormatConfig"`
	httpServer       *http.Server
	incomingRecords  chan []api.Record
}

type RawHttpConfigs struct {
	Port                      float64                  `ConfigDef:"type=NUMBER,required=true"`
	AppId                     string                   `ConfigDef:"type=STRING,required=true"`
	AppIdViaQueryParamAllowed bool                     `ConfigDef:"type=BOOLEAN,required=true"`
	TlsConfigBean             httpcommon.TlsConfigBean `ConfigDefBean:"tlsConfigBean"`
}

func init() {
	stagelibrary.SetCreator(Library, StageName, func() api.Stage {
		return &Origin{BaseStage: &common.BaseStage{}}
	})
}

func (h *Origin) Init(stageContext api.StageContext) []validation.Issue {
	issues := h.BaseStage.Init(stageContext)

	if h.HttpConfigs.TlsConfigBean.TlsEnabled {
		if len(h.HttpConfigs.TlsConfigBean.KeyStoreFilePath) == 0 {
			issues = append(issues, stageContext.CreateConfigIssue(
				"Keystore File path is missing",
				"TLS",
				"httpConfigs.tlsConfigBean.keyStoreFilePath",
			))
		}

		if h.HttpConfigs.TlsConfigBean.KeyStoreType != PKCS12 {
			issues = append(issues, stageContext.CreateConfigIssue(
				"Edge supports only PKCS-12 (p12 file) Key Type",
				"TLS",
				"httpConfigs.tlsConfigBean.keyStoreType",
			))
		}
	}

	h.DataFormatConfig.Init(h.DataFormat, h.GetStageContext(), issues)

	if len(issues) == 0 {
		h.httpServer = h.startHttpServer()
		h.incomingRecords = make(chan []api.Record)
	}

	return issues
}

func (h *Origin) Destroy() error {
	if h.incomingRecords != nil {
		close(h.incomingRecords)
	}
	if h.httpServer != nil {
		if err := h.httpServer.Shutdown(context.Background()); err != nil {
			return err
		}
		log.Debug("HTTP Server - server shutdown successfully")
	}
	return nil
}

func (h *Origin) Produce(
	lastSourceOffset *string,
	maxBatchSize int,
	batchMaker api.BatchMaker,
) (*string, error) {
	log.Debug("HTTP Server - Produce method")
	records := <-h.incomingRecords
	for _, record := range records {
		batchMaker.AddRecord(record)
	}
	return &stringOffset, nil
}

func (h *Origin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.validateAppId(w, r) {
		recordReaderFactory := h.DataFormatConfig.RecordReaderFactory
		recordReader, err := recordReaderFactory.CreateReader(h.GetStageContext(), r.Body, "http-server")
		if err != nil {
			log.WithError(err).Error("Failed to create record reader")
			return
		}
		defer recordReader.Close()

		records := make([]api.Record, 0)

		for {
			record, err := recordReader.ReadRecord()
			if err != nil {
				log.WithError(err).Error("Failed to parse raw data")
				h.GetStageContext().ReportError(err)
			}

			if record == nil {
				break
			}
			records = append(records, record)
		}

		if len(records) > 0 {
			h.incomingRecords <- records
		}
	}
}

func (h *Origin) validateAppId(w http.ResponseWriter, r *http.Request) bool {
	valid := false
	reqAppId := r.Header.Get(X_SDC_APPLICATION_ID_HEADER)
	if len(reqAppId) == 0 && h.HttpConfigs.AppIdViaQueryParamAllowed {
		queryAppId := r.URL.Query()[SDC_APPLICATION_ID_QUERY_PARAM]
		if len(queryAppId) > 0 {
			reqAppId = queryAppId[0]
		}
	}

	if reqAppId != h.HttpConfigs.AppId {
		log.Warnf("Request from '%s' invalid appId '%s', rejected", r.RemoteAddr, reqAppId)
		w.WriteHeader(http.StatusForbidden)
		_, _ = fmt.Fprintf(w, "Invalid 'appId'")
	} else {
		valid = true
	}

	return valid
}

func (h *Origin) startHttpServer() *http.Server {
	srv := &http.Server{
		Addr:    ":" + strconv.FormatFloat(h.HttpConfigs.Port, 'f', 0, 64),
		Handler: h,
	}

	go func() {
		if h.HttpConfigs.TlsConfigBean.TlsEnabled {
			log.Info("HTTP Server Origin - Running on URI : https://localhost:", h.HttpConfigs.Port)
			tlsConfig := h.HttpConfigs.TlsConfigBean

			data, err := ioutil.ReadFile(tlsConfig.KeyStoreFilePath)
			if err != nil {
				log.WithError(err).Error("Failed to KeyStoreFilePath")
				h.GetStageContext().ReportError(err)
				return
			}

			privateKey, certificate, err := pkcs12.Decode(data, tlsConfig.KeyStorePassword)
			if err != nil {
				log.WithError(err).Error("Failed to decode pkcs12 file")
				h.GetStageContext().ReportError(err)
				return
			}

			var certPemBlockBuffer bytes.Buffer
			err = pem.Encode(&certPemBlockBuffer, &pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw})
			if err != nil {
				log.WithError(err).Error("Failed during pem encoding of certificate")
				h.GetStageContext().ReportError(err)
				return
			}

			pk := privateKey.(*rsa.PrivateKey)
			var keyPemBlockBuffer bytes.Buffer
			err = pem.Encode(&keyPemBlockBuffer, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(pk)})
			if err != nil {
				log.WithError(err).Error("Failed during pem encoding of private key")
				h.GetStageContext().ReportError(err)
				return
			}

			srv.TLSConfig = &tls.Config{}
			srv.TLSConfig.Certificates = make([]tls.Certificate, 1)
			srv.TLSConfig.Certificates[0], err = tls.X509KeyPair(certPemBlockBuffer.Bytes(), keyPemBlockBuffer.Bytes())
			if err != nil {
				log.WithError(err).Error("Failed during loading key and certificate to TLS Config")
				h.GetStageContext().ReportError(err)
				return
			}

			if err := srv.ListenAndServeTLS("", ""); err != nil {
				log.WithError(err).Error("HttpServer: ListenAndServe() error")
				h.GetStageContext().ReportError(err)
			}
		} else {
			log.Debug("HTTP Server - Running on URI : http://localhost:", h.HttpConfigs.Port)
			if err := srv.ListenAndServe(); err != nil {
				log.WithError(err).Error("HttpServer: ListenAndServe() error")
				h.GetStageContext().ReportError(err)
			}
		}
	}()

	return srv
}

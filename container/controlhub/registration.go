/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package controlhub

import (
	"bytes"
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/container/common"
	"net/http"
	"runtime"
)

const (
	REGISTRATION_URL_PATH = "/security/public-rest/v1/components/registration"
)

type Attributes struct {
	BaseHttpUrl     string `json:"baseHttpUrl"`
	Sdc2GoGoVersion string `json:"sdc2goGoVersion"`
	Sdc2GoGoOS      string `json:"sdc2goGoOS"`
	Sdc2GoGoArch    string `json:"sdc2goGoArch"`
	Sdc2GoBuildDate string `json:"sdc2goBuildDate"`
	Sdc2GoRepoSha   string `json:"sdc2goRepoSha"`
	Sdc2GoVersion   string `json:"sdc2goVersion"`
}

type RegistrationData struct {
	AuthToken   string     `json:"authToken"`
	ComponentId string     `json:"componentId"`
	Attributes  Attributes `json:"attributes"`
}

func RegisterWithDPM(
	schConfig Config,
	buildInfo *common.BuildInfo,
	runtimeInfo *common.RuntimeInfo,
) {
	if schConfig.Enabled && schConfig.AppAuthToken != "" {
		attributes := Attributes{
			BaseHttpUrl:     runtimeInfo.HttpUrl,
			Sdc2GoGoVersion: runtime.Version(),
			Sdc2GoGoOS:      runtime.GOOS,
			Sdc2GoGoArch:    runtime.GOARCH,
			Sdc2GoBuildDate: buildInfo.BuiltDate,
			Sdc2GoRepoSha:   buildInfo.BuiltRepoSha,
			Sdc2GoVersion:   buildInfo.Version,
		}

		registrationData := RegistrationData{
			AuthToken:   schConfig.AppAuthToken,
			ComponentId: runtimeInfo.ID,
			Attributes:  attributes,
		}

		jsonValue, err := json.Marshal(registrationData)
		if err != nil {
			log.Println(err)
		}

		var registrationUrl = schConfig.BaseUrl + REGISTRATION_URL_PATH

		req, err := http.NewRequest("POST", registrationUrl, bytes.NewBuffer(jsonValue))
		req.Header.Set(common.HEADER_X_REST_CALL, "SDC Edge")
		req.Header.Set(common.HEADER_CONTENT_TYPE, common.APPLICATION_JSON)

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()

		log.WithField("status", resp.Status).Info("DPM Registration Status")
		if resp.StatusCode != 200 {
			log.Panic("DPM Registration failed")
		}
		runtimeInfo.DPMEnabled = true
		runtimeInfo.AppAuthToken = schConfig.AppAuthToken
	} else {
		runtimeInfo.DPMEnabled = false
	}
}

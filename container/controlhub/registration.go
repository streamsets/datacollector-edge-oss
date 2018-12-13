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
package controlhub

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cast"
	"github.com/streamsets/datacollector-edge/container/common"
	"io"
	"io/ioutil"
	"net/http"
	"runtime"
	"strings"
)

const (
	RegistrationUrlPath    = "/security/public-rest/v1/components/registration"
	LoginUrlPath           = "/security/public-rest/v1/authentication/login"
	CreateComponentUrlPath = "/security/rest/v1/organization/%s/components"
	EdgeComponentType      = "dc-edge"
	FullAuthTokenProp      = "fullAuthToken"
	PostRequest            = "POST"
	PutRequest             = "PUT"
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

func RegisterWithControlHub(
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

		var registrationUrl = schConfig.BaseUrl + RegistrationUrlPath

		req, err := http.NewRequest(PostRequest, registrationUrl, bytes.NewBuffer(jsonValue))
		req.Header.Set(common.HeaderXRestCall, EdgeComponentType)
		req.Header.Set(common.HeaderContentType, common.ApplicationJson)

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()

		log.WithField("status", resp.Status).Info("Control Hub Registration Status")
		if resp.StatusCode != 200 {
			log.Panic("Control Hub Registration failed")
		}
		runtimeInfo.DPMEnabled = true
		runtimeInfo.AppAuthToken = schConfig.AppAuthToken
	} else {
		runtimeInfo.DPMEnabled = false
	}
}

func EnableControlHub(
	controlHubUrl string,
	controlHubUser string,
	controlHubPassword string,
	controlHubUserToken string,
) (string, error) {
	var err error

	if len(controlHubPassword) > 0 && len(controlHubUserToken) > 0 {
		return "", errors.New("provide either Control Hub password or user token, but not both")
	}

	if len(controlHubUserToken) == 0 {
		controlHubUserToken, err = retrieveUserToken(controlHubUrl, controlHubUser, controlHubPassword)
		if err != nil {
			return "", err
		}
	}

	orgId := getControlHubOrgId(controlHubUser)
	newComponentJson := map[string]interface{}{
		"organization":       orgId,
		"componentType":      EdgeComponentType,
		"numberOfComponents": 1,
		"active":             true,
	}

	jsonValue, err := json.Marshal(newComponentJson)
	if err != nil {
		return "", err
	}

	var createComponentUrl = controlHubUrl + fmt.Sprintf(CreateComponentUrlPath, orgId)

	req, err := http.NewRequest(PutRequest, createComponentUrl, bytes.NewBuffer(jsonValue))
	req.Header.Set(common.HeaderXRestCall, EdgeComponentType)
	req.Header.Set(common.HeaderContentType, common.ApplicationJson)
	req.Header.Set(common.HeaderXUserAuthToken, controlHubUserToken)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	log.WithField("status", resp.Status).Info("Control Hub token creation status")
	if resp.StatusCode != 201 {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		err = fmt.Errorf("control hub token creation failed: %s", string(bodyBytes))
		return "", err
	}

	decoder := json.NewDecoder(resp.Body)
	var responseList []map[string]interface{}
	err = decoder.Decode(&responseList)
	if err != nil {
		switch {
		case err == io.EOF:
			// empty body
		case err != nil:
			// other error
			return "", fmt.Errorf("parsing Control Hub response failed: %s", err)
		}
	}

	if len(responseList) != 1 {
		return "", errors.New("invalid response from Control Hub")
	}

	fullAuthToken := responseList[0][FullAuthTokenProp]

	return cast.ToString(fullAuthToken), err
}

func getControlHubOrgId(controlHubUser string) string {
	strArr := strings.Split(controlHubUser, "@")
	if len(strArr) < 2 {
		panic("Invalid Control Hub User Id")
	}
	return strArr[1]
}

func retrieveUserToken(
	controlHubUrl string,
	controlHubUser string,
	controlHubPassword string,
) (string, error) {
	var err error

	loginJson := map[string]string{
		"userName": controlHubUser,
		"password": controlHubPassword,
	}

	jsonValue, err := json.Marshal(loginJson)
	if err != nil {
		return "", err
	}

	var loginUrl = controlHubUrl + LoginUrlPath

	req, err := http.NewRequest(PostRequest, loginUrl, bytes.NewBuffer(jsonValue))
	req.Header.Set(common.HeaderXRestCall, EdgeComponentType)
	req.Header.Set(common.HeaderContentType, common.ApplicationJson)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)

	log.WithField("status", resp.Status).Info("Control Hub authentication status")
	if resp.StatusCode != 200 {
		err = fmt.Errorf("control hub authentication failed: %s", string(bodyBytes))
		return "", err
	}
	return resp.Header.Get(common.HeaderXUserAuthToken), nil
}

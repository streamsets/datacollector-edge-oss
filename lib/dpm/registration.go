package dpm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/streamsets/dataextractor/lib/common"
	"io/ioutil"
	"net/http"
	"runtime"
)

type Attributes struct {
	BaseHttpUrl  string `json:"baseHttpUrl"`
	SdeGoVersion string `json:"sdeGoVersion"`
	SdeGoOS      string `json:"sdeGoOS"`
	SdeGoArch    string `json:"sdeGoArch"`
	SdeBuildDate string `json:"sdeBuildDate"`
	SdeRepoSha   string `json:"sdeRepoSha"`
	SdeVersion   string `json:"sdeVersion"`
}

type RegistrationData struct {
	AuthToken   string     `json:"authToken"`
	ComponentId string     `json:"componentId"`
	Attributes  Attributes `json:"attributes"`
}

func RegisterWithDPM(dpmConfig Config, buildInfo *common.BuildInfo, runtimeInfo *common.RuntimeInfo) {
	fmt.Println(dpmConfig)
	if dpmConfig.Enabled && dpmConfig.AppAuthToken != "" {
		attributes := Attributes{
			BaseHttpUrl:  runtimeInfo.HttpUrl,
			SdeGoVersion: runtime.Version(),
			SdeGoOS:      runtime.GOOS,
			SdeGoArch:    runtime.GOARCH,
			SdeBuildDate: buildInfo.BuiltDate,
			SdeRepoSha:   buildInfo.BuiltRepoSha,
			SdeVersion:   buildInfo.Version,
		}

		registrationData := RegistrationData{
			AuthToken:   dpmConfig.AppAuthToken,
			ComponentId: runtimeInfo.ID,
			Attributes:  attributes,
		}

		jsonValue, err := json.Marshal(registrationData)
		if err != nil {
			fmt.Println(err)
		}

		var registrationUrl = dpmConfig.BaseUrl + "/security/public-rest/v1/components/registration"

		req, err := http.NewRequest("POST", registrationUrl, bytes.NewBuffer(jsonValue))
		req.Header.Set("X-Requested-By", "SDE")
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()

		fmt.Println("DPM Registration Status:", resp.Status)
		if resp.StatusCode != 200 {
			panic("DPM Registration failed")
		}
		body, _ := ioutil.ReadAll(resp.Body)
		fmt.Println("response Body:", string(body))
		runtimeInfo.DPMEnabled = true


		// TODO: Fix Events
		// SendEvent(dpmConfig, buildInfo, runtimeInfo)

	} else {
		runtimeInfo.DPMEnabled = false
	}
}

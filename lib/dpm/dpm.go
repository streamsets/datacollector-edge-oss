package dpm

import (
	"github.com/streamsets/dataextractor/lib/common"
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"encoding/json"
	"runtime"
)

type dpmAttributes struct {
	BaseHttpUrl string `json:"baseHttpUrl"`
	SdeGoVersion string `json:"sdeGoVersion"`
	SdeGoOS string `json:"sdeGoOS"`
	SdeGoArch string `json:"sdeGoArch"`
	SdeBuildDate string `json:"sdeBuildDate"`
	SdeRepoSha string `json:"sdeRepoSha"`
	SdeVersion string `json:"sdeVersion"`
}

type registrationData struct {
	AuthToken string `json:"authToken"`
	ComponentId string `json:"componentId"`
	Attributes dpmAttributes `json:"attributes"`
}

func RegisterWithDPM(dpmConfig Config, buildInfo *common.BuildInfo, runtimeInfo *common.RuntimeInfo)  {
	fmt.Println(dpmConfig.Enabled);
	if (dpmConfig.Enabled && dpmConfig.AppAuthToken != "") {
		attributes := dpmAttributes{
			BaseHttpUrl: runtimeInfo.HttpUrl,
			SdeGoVersion: runtime.Version(),
			SdeGoOS: runtime.GOOS,
			SdeGoArch: runtime.GOARCH,
			SdeBuildDate: buildInfo.BuiltDate,
			SdeRepoSha: buildInfo.BuiltRepoSha,
			SdeVersion: buildInfo.Version,
		}

		registrationData := registrationData{
			AuthToken: dpmConfig.AppAuthToken,
			ComponentId: runtimeInfo.ID,
			Attributes: attributes,
		}

		jsonValue, err := json.Marshal(registrationData)
		if (err != nil) {
			fmt.Println(err)
		}

		var dpmRegistrationUrl = dpmConfig.BaseUrl + "/security/public-rest/v1/components/registration"

		req, err := http.NewRequest("POST", dpmRegistrationUrl, bytes.NewBuffer(jsonValue))
		req.Header.Set("X-Requested-By", "SDE")
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()

		fmt.Println("DPM Registration Status:", resp.Status)
		if (resp.StatusCode != 200) {
			panic("DPM Registration failed")
		}
		body, _ := ioutil.ReadAll(resp.Body)
		fmt.Println("response Body:", string(body))
		runtimeInfo.DPMEnabled = true
	} else {
		runtimeInfo.DPMEnabled = false
	}
}

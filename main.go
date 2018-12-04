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
package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/kardianos/service"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cast"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/controlhub"
	"github.com/streamsets/datacollector-edge/container/edge"
	_ "github.com/streamsets/datacollector-edge/stages/destinations"
	_ "github.com/streamsets/datacollector-edge/stages/origins"
	_ "github.com/streamsets/datacollector-edge/stages/processors"
	_ "github.com/streamsets/datacollector-edge/stages/services"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
)

const (
	ServiceStatus = "status"
)

var debugFlag = flag.Bool("debug", false, "Debug flag")
var logToConsoleFlag = flag.Bool("logToConsole", false, "Log to console flag")
var startFlag = flag.String("start", "", "Start Pipeline ID")
var runtimeParametersArg = flag.String("runtimeParameters", "", "Runtime Parameters")
var insecureSkipVerifyArg = flag.Bool(
	"insecureSkipVerify",
	false,
	"InsecureSkipVerify controls whether a client verifies the server's certificate chain and host name",
)
var serviceArg = flag.String(
	"service", "",
	"Manage service commands - install, uninstall, start, stop and restart",
)

var enableControlHubArg = flag.Bool(
	"enableControlHub",
	false,
	"Enable Control Hub",
)

var disableControlHubArg = flag.Bool(
	"disableControlHub",
	false,
	"Disable Control Hub",
)

var controlHubUrlArg = flag.String(
	"controlHubUrl",
	"https://cloud.streamsets.com",
	"Control Hub URl, For Control Hub cloud, use https://cloud.streamsets.com. "+
		"For Control Hub on-premises, use the URL provided by your system administrator. "+
		"For example, https://<hostname>:18631",
)

var controlHubUserTokenArg = flag.String(
	"controlHubUserToken",
	"",
	"Enter your Control Hub user auth token",
)

var controlHubUserArg = flag.String(
	"controlHubUser",
	"",
	"Enter your Control Hub user ID using the following format: <ID>@<organization ID>",
)

var controlHubPasswordArg = flag.String(
	"controlHubPassword",
	"",
	"Enter the password for your Control Hub user account",
)

var controlHubLabelsArg = flag.String(
	"controlHubLabels",
	"",
	"Enter labels to report to Control Hub",
)

type program struct {
	dataCollectorEdge *edge.DataCollectorEdgeMain
}

func (p *program) Start(s service.Service) error {
	go p.run()
	return nil
}

func (p *program) run() {

	fmt.Println("StreamSets Data Collector Edge (SDC Edge): ")
	fmt.Printf("OS: %s\nArchitecture: %s\n", runtime.GOOS, runtime.GOARCH)

	if *insecureSkipVerifyArg {
		tr := http.DefaultTransport.(*http.Transport)
		tr.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
		log.Warn("TLS accepts any certificate presented by the server and any host name in that certificate. " +
			"In this mode, TLS is susceptible to man-in-the-middle attacks. This should be used only for testing")
	}

	p.dataCollectorEdge, _ = edge.DoMain(
		getBaseDir(),
		*debugFlag,
		*logToConsoleFlag,
		*startFlag,
		*runtimeParametersArg,
	)
	go shutdownHook(p.dataCollectorEdge)
	p.dataCollectorEdge.WebServerTask.Run()
}

func (p *program) Stop(s service.Service) error {
	return nil
}

func main() {
	flag.Parse()

	svcConfig := &service.Config{
		Name:        "datacollector-edge",
		DisplayName: "StreamSets Data Collector Edge Service",
		Description: "Streams data such as logs and files for analytics",
	}

	prg := &program{}
	newService, err := service.New(prg, svcConfig)
	if err != nil {
		log.Fatal(err)
	}

	if *serviceArg != "" {
		if *serviceArg == ServiceStatus {
			status, err := newService.Status()
			if err != nil {
				fmt.Println(err)
			} else {
				switch status {
				case service.StatusRunning:
					fmt.Println("Data Collector Edge service is running")
				case service.StatusStopped:
					fmt.Println("Data Collector Edge service is stopped")
				default:
					fmt.Println("Data Collector Edge service is not installed")
				}
			}
		} else {
			err := service.Control(newService, *serviceArg)
			if err != nil {
				fmt.Println(err.Error())
			} else {
				fmt.Printf("Action '%s' for service 'Data Collector Edge' ran successfully. \n", *serviceArg)
			}
		}
	} else if *enableControlHubArg {
		if fullAuthToken, err := controlhub.EnableControlHub(
			*controlHubUrlArg,
			*controlHubUserArg,
			*controlHubPasswordArg,
			*controlHubUserTokenArg,
		); err != nil {
			fmt.Println(err.Error())
		} else {
			edgeConfigFile := getBaseDir() + edge.DefaultConfigFilePath
			config := edge.NewConfig()
			err = config.FromTomlFile(edgeConfigFile)
			if err != nil {
				panic(err)
			}
			config.SCH.Enabled = true
			config.SCH.BaseUrl = *controlHubUrlArg
			config.SCH.AppAuthToken = cast.ToString(fullAuthToken)
			if len(*controlHubLabelsArg) > 0 {
				labels := strings.Split(*controlHubLabelsArg, ",")
				if len(labels) > 0 {
					config.SCH.JobLabels = labels
				}
			}

			if err = config.ToTomlFile(edgeConfigFile); err != nil {
				panic(err)
			}
			fmt.Println("Control Hub enabled successfully")
		}
	} else if *disableControlHubArg {
		edgeConfigFile := getBaseDir() + edge.DefaultConfigFilePath
		config := edge.NewConfig()
		err = config.FromTomlFile(edgeConfigFile)
		if err != nil {
			panic(err)
		}
		config.SCH.Enabled = false
		if err = config.ToTomlFile(edgeConfigFile); err != nil {
			panic(err)
		}
		fmt.Println("Control Hub disabled successfully")
	} else {
		err = newService.Run()
		if err != nil {
			panic(err)
		}
	}
}

func shutdownHook(dataCollectorEdge *edge.DataCollectorEdgeMain) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	log.Infof("Program got a system signal %v", <-c)
	if pipelineInfos, er := dataCollectorEdge.PipelineStoreTask.GetPipelines(); er == nil {
		for _, pipelineInfo := range pipelineInfos {
			runner := dataCollectorEdge.Manager.GetRunner(pipelineInfo.PipelineId)
			if pipelineState, er := runner.GetStatus(); er == nil &&
				(pipelineState.Status == common.RUNNING || pipelineState.Status == common.STARTING) {
				log.WithField("id", pipelineInfo.PipelineId).Info("Stopping pipeline")
				if _, err := runner.StopPipeline(); err != nil {
					log.WithField("id", pipelineInfo.PipelineId).Error("Error stopping pipeline")
				}
			}
		}
	}
	dataCollectorEdge.WebServerTask.Shutdown()
	if dataCollectorEdge.RuntimeInfo.DPMEnabled {
		dataCollectorEdge.DPMMessageEventHandler.Shutdown()
	}
	log.Info("Data Collector Edge shutting down")
}

func getBaseDir() string {
	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	baseDir := strings.TrimSuffix(filepath.Dir(ex), "/bin")
	baseDir = strings.TrimSuffix(baseDir, "\\bin") // for windows
	return baseDir
}

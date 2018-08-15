package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/kardianos/service"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/container/common"
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

var debugFlag = flag.Bool("debug", false, "Debug flag")
var logToConsoleFlag = flag.Bool("logToConsole", false, "Log to console flag")
var startFlag = flag.String("start", "", "Start Pipeline ID")
var runtimeParametersArg = flag.String("runtimeParameters", "", "Runtime Parameters")
var logDirArg = flag.String("logDir", "", "SDC Edge log directory")
var insecureSkipVerifyArg = flag.Bool(
	"insecureSkipVerify",
	false,
	"InsecureSkipVerify controls whether a client verifies the server's certificate chain and host name",
)
var serviceArg = flag.String(
	"service", "",
	"Manage service commands - install, uninstall, start, stop and restart",
)

type program struct {
	dataCollectorEdge *edge.DataCollectorEdgeMain
}

func (p *program) Start(s service.Service) error {
	go p.run()
	return nil
}

func (p *program) run() {
	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	baseDir := strings.TrimSuffix(filepath.Dir(ex), "/bin")
	baseDir = strings.TrimSuffix(baseDir, "\\bin") // for windows

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
		baseDir,
		*debugFlag,
		*logToConsoleFlag,
		*startFlag,
		*runtimeParametersArg,
		*logDirArg,
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
		err := service.Control(newService, *serviceArg)
		if err != nil {
			fmt.Println(err.Error())
		} else {
			fmt.Printf("Action '%s' for service 'datacollector-edge' ran successfully", *serviceArg)
		}
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
				if runner.StopPipeline(); er != nil {
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

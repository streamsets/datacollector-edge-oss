package main

import (
	"crypto/tls"
	"flag"
	"fmt"
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

func main() {
	debugFlag := flag.Bool("debug", false, "Debug flag")
	logToConsoleFlag := flag.Bool("logToConsole", false, "Log to console flag")
	startFlag := flag.String("start", "", "Start Pipeline ID")
	runtimeParametersArg := flag.String("runtimeParameters", "", "Runtime Parameters")
	logDirArg := flag.String("logDir", "", "SDC Edge log directory")
	insecureSkipVerifyArg := flag.Bool(
		"insecureSkipVerify",
		false,
		"InsecureSkipVerify controls whether a client verifies the server's certificate chain and host name",
	)
	flag.Parse()

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

	dataCollectorEdge, _ := edge.DoMain(
		baseDir,
		*debugFlag,
		*logToConsoleFlag,
		*startFlag,
		*runtimeParametersArg,
		*logDirArg,
	)
	go shutdownHook(dataCollectorEdge)
	dataCollectorEdge.WebServerTask.Run()
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

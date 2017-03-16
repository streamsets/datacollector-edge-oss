package tail_dataextractor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/hpcloud/tail"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
)

type Configuration struct {
	FileToTail string
	SDCHttpUrl string
	AppId      string
	Debug      bool
}

type TailDataExtractor struct {
	logger *log.Logger
	config Configuration
	tail   *tail.Tail
}

func (tailDataExtractor *TailDataExtractor) init() {
	configuration, err := loadConfig()
	if err != nil {
		panic(err)
	}
	tailDataExtractor.config = configuration
}

func (tailDataExtractor *TailDataExtractor) Start(offset string) {
	fmt.Println("Started tailing file: " + tailDataExtractor.config.FileToTail)

	tailConfig := tail.Config{Follow: true}

	if offset != "" {
		intOffset, _ := strconv.ParseInt(offset, 10, 64)
		tailConfig.Location = &tail.SeekInfo{Offset: intOffset}
		fmt.Println("Started Offset: ")
		fmt.Println(tailConfig.Location.Offset)
	}

	t, err := tail.TailFile(tailDataExtractor.config.FileToTail, tailConfig)

	if err != nil {
		fmt.Println("error:", err)
		panic(err)
	}

	tailDataExtractor.tail = t

	for line := range t.Lines {
		tailDataExtractor.sendLineToSDC(line.Text)
	}
}

func (tailDataExtractor *TailDataExtractor) sendLineToSDC(line string) {
	if tailDataExtractor.config.Debug {
		fmt.Println("Start sending line")
		fmt.Println(line)
		fmt.Println("URL:>", tailDataExtractor.config.SDCHttpUrl)
	}

	var logTextStr = []byte(line)
	req, err := http.NewRequest("POST", tailDataExtractor.config.SDCHttpUrl, bytes.NewBuffer(logTextStr))
	req.Header.Set("X-SDC-APPLICATION-ID", tailDataExtractor.config.AppId)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	if tailDataExtractor.config.Debug {
		fmt.Println("response Status:", resp.Status)
		fmt.Println("response Headers:", resp.Header)
		body, _ := ioutil.ReadAll(resp.Body)
		fmt.Println("response Body:", string(body))
	}
}

func (tailDataExtractor *TailDataExtractor) Stop() (string, error) {
	fmt.Println("Stopping TailDataExtractor ....")

	offset, _ := tailDataExtractor.tail.Tell()

	err := tailDataExtractor.tail.Stop()
	if err != nil {
		fmt.Println("Stop error:", err)
	}

	return strconv.FormatInt(offset, 10), err
}

func New(logger *log.Logger) (*TailDataExtractor, error) {
	tailDataExtractor := TailDataExtractor{logger: logger}
	tailDataExtractor.init()
	return &tailDataExtractor, nil
}

func loadConfig() (Configuration, error) {
	configuration := Configuration{}
	file, err := os.Open("etc/conf.json")
	if err != nil {
		return configuration, err
	}

	decoder := json.NewDecoder(file)
	err1 := decoder.Decode(&configuration)
	if err1 != nil {
		return configuration, err1
	}
	fmt.Println("Using Configuration")
	fmt.Println(configuration)
	return configuration, err1
}

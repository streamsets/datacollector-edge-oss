package store

import (
	"encoding/json"
	"errors"
	"github.com/satori/go.uuid"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	pipelineStateStore "github.com/streamsets/datacollector-edge/container/execution/store"
	"io/ioutil"
	"os"
	"time"
)

const (
	PIPELINE_FILE             = "pipeline.json"
	PIPELINE_INFO_FILE        = "info.json"
	PIPELINES_FOLDER          = "/data/pipelines/"
	PIPELINES_RUN_INFO_FOLDER = "/data/runInfo/"
)

type FilePipelineStoreTask struct {
	runtimeInfo common.RuntimeInfo
}

func (store *FilePipelineStoreTask) GetPipelines() ([]common.PipelineInfo, error) {
	var pipelineInfoList []common.PipelineInfo
	files, err := ioutil.ReadDir(store.runtimeInfo.BaseDir + PIPELINES_FOLDER)

	if err != nil {
		return nil, err
	}

	for _, f := range files {
		if f.IsDir() {
			pipelineInfo := common.PipelineInfo{}
			file, err := os.Open(store.getPipelineInfoFile(f.Name()))
			if err != nil {
				return nil, err
			}

			decoder := json.NewDecoder(file)
			err = decoder.Decode(&pipelineInfo)
			if err != nil {
				return nil, err
			}
			pipelineInfoList = append(pipelineInfoList, pipelineInfo)
		}
	}

	return pipelineInfoList, nil
}

func (store *FilePipelineStoreTask) GetInfo(pipelineId string) (common.PipelineInfo, error) {
	if !store.hasPipeline(pipelineId) {
		return common.PipelineInfo{}, errors.New("Pipeline '" + pipelineId + " does not exist")
	}

	pipelineInfo := common.PipelineInfo{}
	file, err := os.Open(store.getPipelineInfoFile(pipelineId))
	if err != nil {
		return pipelineInfo, err
	}

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&pipelineInfo)
	if err != nil {
		return pipelineInfo, err
	}

	if pipelineInfo.PipelineId == "" {
		err = errors.New("InValid pipeline configuration")
	}

	return pipelineInfo, err
}

func (store *FilePipelineStoreTask) Create(
	pipelineId string,
	pipelineTitle string,
	description string,
	isRemote bool,
) (common.PipelineConfiguration, error) {

	if store.hasPipeline(pipelineId) {
		return common.PipelineConfiguration{}, errors.New("Pipeline '" + pipelineId + " already exists")
	}

	currentTime := time.Now().Unix()
	metadata := map[string]interface{}{
		"labels": []string{},
	}
	pipelineUuid := uuid.NewV4().String()

	pipelineInfo := common.PipelineInfo{
		PipelineId:   pipelineId,
		Title:        pipelineTitle,
		Description:  description,
		Created:      currentTime,
		LastModified: currentTime,
		Creator:      "admin",
		LastModifier: "admin",
		LastRev:      "0",
		UUID:         pipelineUuid,
		Valid:        true,
		Metadata:     metadata,
	}

	pipelineConfiguration := common.PipelineConfiguration{
		SchemaVersion:        common.PIPELINE_CONFIG_SCHEMA_VERSION,
		Version:              common.PIPELINE_CONFIG_VERSION,
		PipelineId:           pipelineId,
		Title:                pipelineTitle,
		Description:          description,
		UUID:                 pipelineUuid,
		Configuration:        creation.GetDefaultPipelineConfigs(),
		UiInfo:               map[string]interface{}{},
		Stages:               []common.StageConfiguration{},
		ErrorStage:           creation.GetTrashErrorStageInstance(),
		StatsAggregatorStage: creation.GetDefaultStatsAggregatorStageInstance(),
		Previewable:          true,
		Info:                 pipelineInfo,
		Metadata:             metadata,
	}

	err := os.MkdirAll(store.getPipelineDir(pipelineId), 0777)
	if err != nil {
		return pipelineConfiguration, err
	}

	pipelineInfoJson, err := json.MarshalIndent(pipelineInfo, "", "  ")
	if err != nil {
		return pipelineConfiguration, err
	}
	err = ioutil.WriteFile(store.getPipelineInfoFile(pipelineId), pipelineInfoJson, 0644)

	pipelineConfigurationJson, err := json.MarshalIndent(pipelineConfiguration, "", "  ")
	if err != nil {
		return pipelineConfiguration, err
	}
	err = ioutil.WriteFile(store.getPipelineFile(pipelineId), pipelineConfigurationJson, 0644)
	if err != nil {
		return pipelineConfiguration, err
	}

	err = pipelineStateStore.Edited(pipelineId, isRemote)
	return pipelineConfiguration, err
}

func (store *FilePipelineStoreTask) Save(
	pipelineId string,
	pipelineConfiguration common.PipelineConfiguration,
) (common.PipelineConfiguration, error) {
	if !store.hasPipeline(pipelineId) {
		return common.PipelineConfiguration{}, errors.New("Pipeline '" + pipelineId + " does not exist")
	}

	savedInfo, err := store.GetInfo(pipelineId)
	if err != nil {
		return pipelineConfiguration, err
	}

	if savedInfo.UUID != pipelineConfiguration.UUID {
		return pipelineConfiguration, errors.New("The pipeline '" + pipelineId + "' has been changed.")
	}

	currentTime := time.Now().Unix()
	pipelineUuid := uuid.NewV4().String()
	pipelineInfo := pipelineConfiguration.Info

	pipelineInfo.UUID = pipelineUuid
	pipelineInfo.LastModified = currentTime
	pipelineInfo.Title = pipelineConfiguration.Title
	pipelineInfo.Description = pipelineConfiguration.Description

	pipelineConfiguration.Info = pipelineInfo
	pipelineConfiguration.UUID = pipelineUuid

	pipelineInfoJson, err := json.MarshalIndent(pipelineInfo, "", "  ")
	if err != nil {
		return pipelineConfiguration, err
	}
	err = ioutil.WriteFile(store.getPipelineInfoFile(pipelineId), pipelineInfoJson, 0644)

	pipelineConfigurationJson, err := json.MarshalIndent(pipelineConfiguration, "", "  ")
	if err != nil {
		return pipelineConfiguration, err
	}
	err = ioutil.WriteFile(store.getPipelineFile(pipelineId), pipelineConfigurationJson, 0644)

	return pipelineConfiguration, nil
}

func (store *FilePipelineStoreTask) LoadPipelineConfig(pipelineId string) (common.PipelineConfiguration, error) {
	pipelineConfiguration := common.PipelineConfiguration{}
	file, err := os.Open(store.getPipelineFile(pipelineId))
	if err != nil {
		return pipelineConfiguration, err
	}

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&pipelineConfiguration)
	if err != nil {
		return pipelineConfiguration, err
	}

	if pipelineConfiguration.PipelineId == "" {
		err = errors.New("InValid pipeline configuration")
	}

	return pipelineConfiguration, err
}

func (store *FilePipelineStoreTask) Delete(pipelineId string) error {
	if !store.hasPipeline(pipelineId) {
		return errors.New("Pipeline '" + pipelineId + " does not exist")
	}
	err := os.RemoveAll(store.getPipelineDir(pipelineId))
	if err != nil {
		return err
	}
	err = os.RemoveAll(store.getPipelineRunInfoDir(pipelineId))
	return err
}

func (store *FilePipelineStoreTask) hasPipeline(pipelineId string) bool {
	_, err := os.Stat(store.getPipelineDir(pipelineId))
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func (store *FilePipelineStoreTask) getPipelineFile(pipelineId string) string {
	return store.getPipelineDir(pipelineId) + PIPELINE_FILE
}

func (store *FilePipelineStoreTask) getPipelineInfoFile(pipelineId string) string {
	return store.getPipelineDir(pipelineId) + PIPELINE_INFO_FILE
}

func (store *FilePipelineStoreTask) getPipelineDir(pipelineId string) string {
	return store.runtimeInfo.BaseDir + PIPELINES_FOLDER + pipelineId + "/"
}

func (store *FilePipelineStoreTask) getPipelineRunInfoDir(pipelineId string) string {
	return store.runtimeInfo.BaseDir + PIPELINES_RUN_INFO_FOLDER + pipelineId + "/"
}

func NewFilePipelineStoreTask(runtimeInfo common.RuntimeInfo) PipelineStoreTask {
	pipelineStateStore.BaseDir = runtimeInfo.BaseDir
	return &FilePipelineStoreTask{runtimeInfo: runtimeInfo}
}

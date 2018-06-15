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
package system_metrics

import (
	"encoding/json"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/net"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/util"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"time"
)

const (
	Library   = "streamsets-datacollector-basic-lib"
	StageName = "com_streamsets_pipeline_stage_origin_systemmetrics_SystemMetricsDSource"
)

var defaultOffset = "systemMetrics"

type Origin struct {
	*common.BaseStage
	Conf OriginClientConfig `ConfigDefBean:"conf"`
}

type OriginClientConfig struct {
	Delay          float64 `ConfigDef:"type=NUMBER,required=true"`
	FetchHostInfo  bool    `ConfigDef:"type=BOOLEAN,required=true"`
	FetchCpuStats  bool    `ConfigDef:"type=BOOLEAN,required=true"`
	FetchMemStats  bool    `ConfigDef:"type=BOOLEAN,required=true"`
	FetchDiskStats bool    `ConfigDef:"type=BOOLEAN,required=true"`
	FetchNetStats  bool    `ConfigDef:"type=BOOLEAN,required=true"`
}

func init() {
	stagelibrary.SetCreator(Library, StageName, func() api.Stage {
		return &Origin{BaseStage: &common.BaseStage{}}
	})
}

func (o *Origin) Init(stageContext api.StageContext) []validation.Issue {
	issues := o.BaseStage.Init(stageContext)
	return issues
}

func (o *Origin) Produce(lastSourceOffset *string, maxBatchSize int, batchMaker api.BatchMaker) (*string, error) {
	time.Sleep(time.Duration(o.Conf.Delay) * time.Millisecond)

	recordValue := make(map[string]interface{})

	if o.Conf.FetchHostInfo {
		if hostInfoValue, err := o.getHostInfo(); err == nil {
			recordValue["hostInfo"] = hostInfoValue
		} else {
			o.GetStageContext().ReportError(err)
			return &defaultOffset, nil
		}
	}

	if o.Conf.FetchCpuStats {
		if cpuStatsValue, err := o.getCpuStats(); err == nil {
			recordValue["cpu"] = cpuStatsValue
		} else {
			o.GetStageContext().ReportError(err)
			return &defaultOffset, nil
		}
	}

	if o.Conf.FetchMemStats {
		if memStatsValue, err := o.getMemoryStats(); err == nil {
			recordValue["memory"] = memStatsValue
		} else {
			o.GetStageContext().ReportError(err)
			return &defaultOffset, nil
		}
	}

	if o.Conf.FetchDiskStats {
		if diskStatsValue, err := o.getDiskStats("/"); err == nil {
			recordValue["disk"] = diskStatsValue
		} else {
			o.GetStageContext().ReportError(err)
			return &defaultOffset, nil
		}
	}

	if o.Conf.FetchNetStats {
		if netStatsValue, err := o.getNetworkStats(); err == nil {
			recordValue["network"] = netStatsValue
		} else {
			o.GetStageContext().ReportError(err)
			return &defaultOffset, nil
		}
	}

	if record, err := o.GetStageContext().CreateRecord(defaultOffset, recordValue); err == nil {
		timeStampField, _ := api.CreateDateTimeField(util.ConvertTimeToLong(time.Now()))
		record.SetField("/timestamp", timeStampField)
		batchMaker.AddRecord(record)
	} else {
		o.GetStageContext().ReportError(err)
	}
	return &defaultOffset, nil
}

func (o *Origin) getHostInfo() (map[string]interface{}, error) {
	hostInfoValue := make(map[string]interface{})
	if hostInfoStat, err := host.Info(); err == nil {
		json.Unmarshal([]byte(hostInfoStat.String()), &hostInfoValue)
		return hostInfoValue, nil
	} else {
		return nil, err
	}
}

func (o *Origin) getCpuStats() (map[string]interface{}, error) {
	cpuStatsValue := make(map[string]interface{})

	if cpuPercentage, err := cpu.Percent(1, false); err == nil {
		cpuStatsValue["percentage"] = cpuPercentage
	} else {
		return nil, err
	}

	if timesStatList, err := cpu.Times(false); err == nil {
		timeStat := timesStatList[0]
		timeStatValue := make(map[string]interface{})
		if err = json.Unmarshal([]byte(timeStat.String()), &timeStatValue); err == nil {
			cpuStatsValue["timeStat"] = timeStatValue
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}

	if infoStatList, err := cpu.Info(); err == nil {
		infoStatListValue := make([]map[string]interface{}, len(infoStatList))
		for i, infoStat := range infoStatList {
			infoStatValue := make(map[string]interface{})
			json.Unmarshal([]byte(infoStat.String()), &infoStatValue)
			infoStatListValue[i] = infoStatValue
		}
		cpuStatsValue["infoStat"] = infoStatListValue
	} else {
		return nil, err
	}

	return cpuStatsValue, nil
}

func (o *Origin) getDiskStats(path string) (map[string]interface{}, error) {
	diskStatsValue := make(map[string]interface{})

	if partitionStatList, err := disk.Partitions(true); err == nil {
		partitionStatsValue := make([]map[string]interface{}, len(partitionStatList))
		for i, partitionStat := range partitionStatList {
			partitionStatValue := make(map[string]interface{})
			json.Unmarshal([]byte(partitionStat.String()), &partitionStatValue)
			partitionStatsValue[i] = partitionStatValue
		}
		diskStatsValue["partitionStats"] = partitionStatsValue
	} else {
		return nil, err
	}

	if usageStat, err := disk.Usage(path); err == nil {
		usageStatValue := make(map[string]interface{})
		json.Unmarshal([]byte(usageStat.String()), &usageStatValue)
		diskStatsValue["usageStat"] = usageStatValue
	} else {
		return nil, err
	}

	if ioCounterStatMap, err := disk.IOCounters(); err == nil {
		ioCounterStatMapValue := make(map[string]interface{})
		for key, ioCounterStat := range ioCounterStatMap {
			ioCounterStatValue := make(map[string]interface{})
			json.Unmarshal([]byte(ioCounterStat.String()), &ioCounterStatValue)
			ioCounterStatMapValue[key] = ioCounterStatValue
		}
		diskStatsValue["ioCounterStat"] = ioCounterStatMapValue
	} else {
		return nil, err
	}

	return diskStatsValue, nil
}

func (o *Origin) getMemoryStats() (map[string]interface{}, error) {
	memStatsValue := make(map[string]interface{})

	if virtualMemoryStat, err := mem.VirtualMemory(); err == nil {
		virtualMemoryStatValue := make(map[string]interface{})
		json.Unmarshal([]byte(virtualMemoryStat.String()), &virtualMemoryStatValue)
		memStatsValue["virtualMemoryStat"] = virtualMemoryStatValue
	} else {
		return nil, err
	}

	if swapMemoryStat, err := mem.SwapMemory(); err == nil {
		swapMemoryStatValue := make(map[string]interface{})
		json.Unmarshal([]byte(swapMemoryStat.String()), &swapMemoryStatValue)
		memStatsValue["swapMemoryStat"] = swapMemoryStatValue
	} else {
		return nil, err
	}

	return memStatsValue, nil
}

func (o *Origin) getNetworkStats() (map[string]interface{}, error) {
	netStatsValue := make(map[string]interface{})

	if ioCountersStats, err := net.IOCounters(true); err == nil {
		ioCountersStatsValue := make([]map[string]interface{}, len(ioCountersStats))
		for i, ioCountersStat := range ioCountersStats {
			ioCountersStatValue := make(map[string]interface{})
			json.Unmarshal([]byte(ioCountersStat.String()), &ioCountersStatValue)
			ioCountersStatsValue[i] = ioCountersStatValue
		}
		netStatsValue["ioCountersStat"] = ioCountersStatsValue
	} else {
		return nil, err
	}

	if connectionStats, err := net.Connections("all"); err == nil {
		connectionStatsValue := make([]map[string]interface{}, len(connectionStats))
		for i, connectionStat := range connectionStats {
			connectionStatValue := make(map[string]interface{})
			json.Unmarshal([]byte(connectionStat.String()), &connectionStatValue)
			connectionStatsValue[i] = connectionStatValue
		}
		netStatsValue["connectionStats"] = connectionStatsValue
	} else {
		return nil, err
	}

	return netStatsValue, nil
}

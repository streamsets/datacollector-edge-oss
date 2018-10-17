// +build 386 windows,amd64 windows

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
package rendering

import (
	"errors"
	winevtcommon "github.com/streamsets/datacollector-edge/stages/origins/windows/wineventlog/common"
)

type winEventLogPublisherManager struct {
	providerToPublisherMetadataHandle map[string]winevtcommon.PublisherMetadataHandle
}

func (welpm *winEventLogPublisherManager) GetPublisherHandle(
	provider string,
) (winevtcommon.PublisherMetadataHandle, error) {
	var err error
	providerHandle := winevtcommon.PublisherMetadataHandle(0)
	if provider != "" {
		var ok bool
		providerHandle, ok = welpm.providerToPublisherMetadataHandle[provider]
		if !ok {
			providerHandle, err = winevtcommon.EvtOpenPublisherMetadata(provider)
		}
	} else {
		err = errors.New("invalid arg - provider empty")
	}
	return providerHandle, err
}

func (welpm *winEventLogPublisherManager) Close() {
	for _, publisherMetadataHandle := range welpm.providerToPublisherMetadataHandle {
		publisherMetadataHandle.Close()
	}
}

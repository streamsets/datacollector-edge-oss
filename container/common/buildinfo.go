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
package common

type BuildInfo struct {
	BuiltBy           string `json:"builtBy"`
	BuiltDate         string `json:"builtDate"`
	BuiltRepoSha      string `json:"builtRepoSha"`
	SourceMd5Checksum string `json:"sourceMd5Checksum"`
	Version           string `json:"version"`
}

var (
	BuiltBy           string
	BuiltDate         string
	BuiltRepoSha      string
	SourceMd5Checksum string
	Version           string
)

func NewBuildInfo() (*BuildInfo, error) {
	buildInfo := BuildInfo{}
	buildInfo.BuiltBy = BuiltBy
	buildInfo.BuiltDate = BuiltDate
	buildInfo.BuiltRepoSha = BuiltRepoSha
	buildInfo.SourceMd5Checksum = SourceMd5Checksum
	buildInfo.Version = Version
	return &buildInfo, nil
}

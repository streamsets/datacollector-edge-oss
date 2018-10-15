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

package common

import (
	"golang.org/x/sys/windows"
)

type SIDType uint32

//https://docs.microsoft.com/en-us/windows/desktop/CIMWin32Prov/win32-useraccount
var SIDTypeStringMap = map[SIDType]string{
	SIDType(1): "User",
	SIDType(2): "Group",
	SIDType(3): "Alias",
	SIDType(4): "Well Known Group",
	SIDType(5): "Alias",
	SIDType(6): "Deleted Account",
	SIDType(7): "Unknown",
	SIDType(8): "Computer",
}

func (s SIDType) GetSidTypeString() string {
	if mapping, stringMappingPresent := SIDTypeStringMap[s]; stringMappingPresent {
		return mapping
	}
	return ""
}

type SIDInfo struct {
	Name    string
	Domain  string
	SIDType SIDType
}

func GetSidInfo(sid *windows.SID) (*SIDInfo, error) {
	var sidInfo *SIDInfo
	account, domain, sidType, err := sid.LookupAccount("")
	if err == nil {
		sidInfo = &SIDInfo{Name: account, Domain: domain, SIDType: SIDType(sidType)}
	}
	return sidInfo, err
}

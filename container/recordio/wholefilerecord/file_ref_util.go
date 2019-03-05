// Copyright 2019 StreamSets Inc.
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

package wholefilerecord

import "os"

const (
	FileRefFieldName      = "fileRef"
	FileRefFieldPathName  = "/fileRef"
	FileInfoFieldName     = "fileInfo"
	FileInfoFieldPathName = "/fileInfo"
)

func GetFileInfo(filePath string) (map[string]interface{}, error) {
	fileInfo := make(map[string]interface{})

	fileStat, err := os.Stat(filePath)
	if err != nil {
		return nil, err
	}

	fileMode := fileStat.Mode()

	fileInfo["filename"] = fileStat.Name()
	fileInfo["file"] = filePath
	fileInfo["size"] = fileStat.Size()
	fileInfo["lastModifiedTime"] = fileStat.ModTime()
	fileInfo["permissions"] = fileMode.String()
	fileInfo["isDirectory"] = fileStat.IsDir()
	fileInfo["isRegularFile"] = fileMode.IsRegular()
	fileInfo["isSymbolicLink"] = fileMode&os.ModeSymlink != 0

	return fileInfo, nil
}

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

import (
	"github.com/juju/ratelimit"
	"github.com/streamsets/datacollector-edge/api"
	"io"
	"os"
)

type LocalFileRef struct {
	filePath   string
	bufferSize int64
	rateLimit  float64
}

func (f *LocalFileRef) CreateInputStream() (io.Reader, error) {
	fileReader, err := os.Open(f.filePath)
	if f.rateLimit < 0 {
		return fileReader, err
	} else {
		if err != nil {
			return nil, err
		}
		return ratelimit.Reader(fileReader, ratelimit.NewBucketWithRate(f.rateLimit, f.bufferSize)), nil
	}
}

func (f *LocalFileRef) CloseInputStream(reader io.Reader) error {
	if reader != nil {
		if fileReader, ok := reader.(io.Closer); ok {
			return fileReader.Close()
		}
	}
	return nil
}

func NewLocalFileRef(filePath string, bufferSize int64, rateLimit float64) api.FileRef {
	return &LocalFileRef{
		filePath:   filePath,
		bufferSize: bufferSize,
		rateLimit:  rateLimit,
	}
}

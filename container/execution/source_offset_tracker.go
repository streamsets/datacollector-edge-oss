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
package execution

import "time"

// Offset tracker is for committing and retrieving offsets in a store.
//
// IsFinished Return if the source finished processing data.
// This is more of a historical method as it determines whether source is done reading by checking for special
// offset value. This method will only work (e.g. return true) for (Pull)Source - it will never return true
// for PushSource.
//
// CommitOffset Change offset for entity in the tracked offset and commit it to persistent store.
//
// GetOffset Return currently staged offset.
//
// GetLastBatchTime Get time of lastly committed batch.
type SourceOffsetTracker interface {
	IsFinished() bool

	SetOffset(newOffset *string)

	CommitOffset() error

	GetOffset() *string

	GetLastBatchTime() time.Time
}

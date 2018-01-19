// +build kafka

/*
 * Copyright 2018 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka

import (
	"testing"
)

func TestRoundRobinPartitioner(t *testing.T) {
	pInfo := PartitionInfo{Count: 5}
	topic := "test"
	topicMetadata[topic] = &pInfo

	for i := 0; i < 10; i++ {
		next, err := nextPartition(nil, &topic, ROUND_ROBIN)
		if err != nil {
			t.Error(err)
		}

		if int(next) != (i % 5) {
			t.Errorf("Expected %d but found %d", i, next)
		}
	}
}

func TestRandomPartitioner(t *testing.T) {
	pInfo := PartitionInfo{Count: 4}
	topic := "test"
	topicMetadata[topic] = &pInfo

	for i := 0; i < 10; i++ {
		next, _ := nextPartition(nil, &topic, RANDOM)
		if next < 0 || next > pInfo.Count {
			t.Errorf("Partition was out of range: %d", next)
		}
	}
}

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
	"errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
	"math/rand"
)

type PartitionStrategy int

const (
	RANDOM      PartitionStrategy = iota
	ROUND_ROBIN PartitionStrategy = iota
	EXPRESSION  PartitionStrategy = iota
	DEFAULT     PartitionStrategy = iota
)

type PartitionInfo struct {
	Count int32 // total number of partitions
	Next  int32 // used for round robin partition strategy
}

var topicMetadata = make(map[string]*PartitionInfo)

func getPartitionInfo(producer *kafka.Producer, topic *string) (*PartitionInfo, error) {
	partitionInfo, exists := topicMetadata[*topic]

	if exists {
		return partitionInfo, nil
	}

	metadata, err := producer.GetMetadata(topic, false, 5000)
	if err != nil {
		log.WithError(err).WithField("topic", topic).Error("Failed to fetch topic metadata")
		return nil, err
	}

	partitionInfo = &PartitionInfo{Count: int32(len(metadata.Topics[*topic].Partitions))}
	topicMetadata[*topic] = partitionInfo
	return partitionInfo, nil
}

func nextPartition(producer *kafka.Producer, topic *string, strategy PartitionStrategy) (int32, error) {
	partitionInfo, err := getPartitionInfo(producer, topic)
	if err != nil {
		return -1, err
	}

	switch strategy {
	case DEFAULT:
		return kafka.PartitionAny, nil
	case RANDOM:
		return rand.Int31n(partitionInfo.Count), nil
	case ROUND_ROBIN:
		next := partitionInfo.Next
		partitionInfo.Next = (partitionInfo.Next + 1) % partitionInfo.Count
		return next, nil
	default:
		return -1, errors.New("Unsupported/Unrecognized Partitioner Type")
	}
}

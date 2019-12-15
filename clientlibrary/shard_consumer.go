/*
 * Copyright (c) 2018 VMware, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
// The implementation is derived from https://github.com/patrobinson/gokini
//
// Copyright 2018 Patrick robinson
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
package kcl

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

const (
	// This is the initial state of a shard consumer. This causes the consumer to remain blocked until the all
	// parent shards have been completed.
	WAITING_ON_PARENT_SHARDS ShardConsumerState = iota + 1

	// This state is responsible for initializing the record processor with the shard information.
	INITIALIZING

	//
	PROCESSING

	SHUTDOWN_REQUESTED

	SHUTTING_DOWN

	SHUTDOWN_COMPLETE

	// ErrCodeKMSThrottlingException is defined in the API Reference https://docs.aws.amazon.com/sdk-for-go/api/service/kinesis/#Kinesis.GetRecords
	// But it's not a constant?
	ErrCodeKMSThrottlingException = "KMSThrottlingException"
)

// ExtendedSequenceNumber represents a two-part sequence number for records aggregated by the Kinesis Producer Library.
//
// The KPL combines multiple user records into a single Kinesis record. Each user record therefore has an integer
// sub-sequence number, in addition to the regular sequence number of the Kinesis record. The sub-sequence number
// is used to checkpoint within an aggregated record.
type ExtendedSequenceNumber struct {
	SequenceNumber    *string
	SubSequenceNumber int64
}

type ShardInfo struct {
	ID            string
	ParentShardID string

	// Checkpoint is the last checkpoint.
	Checkpoint   string
	AssignedTo   string
	LeaseTimeout time.Time
	// Shard Range
	StartingSequenceNumber string
	// child shard doesn't have end sequence number
	EndingSequenceNumber string
}

// ShardStatus represents a shard consumer's progress in a shard.
// NB: this type is passed around and mutated.
type ShardStatus struct {
	ID            string
	ParentShardId string

	// Checkpoint is the last checkpoint.
	Checkpoint   string
	AssignedTo   string
	mux          *sync.Mutex
	LeaseTimeout time.Time
	// Shard Range
	StartingSequenceNumber string
	// child shard doesn't have end sequence number
	EndingSequenceNumber string
}

func (ss *ShardStatus) GetLeaseOwner() string {
	ss.mux.Lock()
	defer ss.mux.Unlock()
	return ss.AssignedTo
}

func (ss *ShardStatus) SetLeaseOwner(owner string) {
	ss.mux.Lock()
	defer ss.mux.Unlock()
	ss.AssignedTo = owner
}

type ShardConsumerState int

// ShardConsumer is responsible for consuming data records of a (specified) shard.
type ShardConsumer struct {
	streamName      string
	shard           *ShardStatus
	kc              kinesisiface.KinesisAPI
	checkpointer    Checkpointer
	recordProcessor RecordProcessor
	cfg             *KinesisClientLibConfiguration
	stop            *chan struct{}
	consumerID      string
	metrics         MonitoringService
	state           ShardConsumerState
}

// run continously poll the shard for records, until the lease ends.
// entry point for consumer.
// Precondition: it currently has the lease on the shard.
func (sc *ShardConsumer) run(ctx context.Context) error {
	defer sc.releaseLease()
	log := sc.cfg.Logger
	shard := sc.shard

	// If the shard is child shard, need to wait until the parent finished.
	if err := sc.waitOnParentShard(); err != nil {
		// If parent shard has been deleted by Kinesis system already, just ignore the error.
		if err != ErrSequenceIDNotFound {
			log.Errorf("Error in waiting for parent shard: %v to finish. Error: %+v", shard.ParentShardId, err)
			return err
		}
	}

	shardIterator, err := sc.getShardIterator(ctx)
	if err != nil {
		log.Errorf("Unable to get shard iterator for %s: %v", shard.ID, err)
		return err
	}

	// Start processing events and notify record processor on shard and starting checkpoint
	sc.recordProcessor.Initialize(&InitializationInput{
		ShardID:                shard.ID,
		ExtendedSequenceNumber: &ExtendedSequenceNumber{SequenceNumber: aws.String(shard.Checkpoint)},
	})

	recordCheckpointer := NewRecordProcessorCheckpointer(shard, sc.checkpointer)
	retriedErrors := 0

	// TODO: add timeout
	// each iter: { call GetRecords, call RecordProcessor, check lease status and control accordingly }
	for {

		if time.Now().UTC().After(shard.LeaseTimeout.Add(-time.Duration(sc.cfg.LeaseRefreshPeriodMillis) * time.Millisecond)) {
			log.Debugf("Refreshing lease on shard: %s for worker: %s", shard.ID, sc.consumerID)
			err = sc.checkpointer.GetLease(ctx, shard, sc.consumerID)
			if err != nil {
				if err.Error() == ErrLeaseNotAquired {
					log.Warnf("Failed in acquiring lease on shard: %s for worker: %s", shard.ID, sc.consumerID)
					return nil
				}
				// log and return error
				log.Errorf("Error in refreshing lease on shard: %s for worker: %s. Error: %+v",
					shard.ID, sc.consumerID, err)
				return err
			}
		}

		getRecordsStartTime := time.Now()

		log.Debugf("Trying to read %d record from iterator: %v", sc.cfg.MaxRecords, aws.StringValue(shardIterator))
		getRecordsArgs := &kinesis.GetRecordsInput{
			Limit:         aws.Int64(int64(sc.cfg.MaxRecords)),
			ShardIterator: shardIterator,
		}
		// Get records from stream and retry as needed
		getResp, err := sc.kc.GetRecordsWithContext(ctx, getRecordsArgs)
		if err != nil {
			if awsErr, ok := err.(awserr.Error); ok {
				if awsErr.Code() == kinesis.ErrCodeProvisionedThroughputExceededException || awsErr.Code() == ErrCodeKMSThrottlingException {
					log.Errorf("Error getting records from shard %v: %+v", shard.ID, err)
					retriedErrors++
					// exponential backoff
					// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html#Programming.Errors.RetryAndBackoff
					time.Sleep(time.Duration(math.Exp2(float64(retriedErrors))*100) * time.Millisecond)
					continue
				}
			}
			log.Errorf("Error getting records from Kinesis that cannot be retried: %+v Request: %s", err, getRecordsArgs)
			return err
		}

		// Convert from nanoseconds to milliseconds
		getRecordsTimeMS := time.Since(getRecordsStartTime) / 1000000
		sc.metrics.RecordGetRecordsTime(shard.ID, float64(getRecordsTimeMS))

		// reset the retry count after success
		retriedErrors = 0

		// IRecordProcessorCheckpointer
		input := &ProcessRecordsInput{
			Records:            getResp.Records,
			MillisBehindLatest: aws.Int64Value(getResp.MillisBehindLatest),
			Checkpointer:       recordCheckpointer,
		}

		recordLength := len(input.Records)
		recordBytes := int64(0)
		log.Debugf("Received %d records, MillisBehindLatest: %v", recordLength, input.MillisBehindLatest)

		for _, r := range getResp.Records {
			recordBytes += int64(len(r.Data))
		}

		if recordLength > 0 || sc.cfg.CallProcessRecordsEvenForEmptyRecordList {
			processRecordsStartTime := time.Now()

			// Delivery the events to the record processor
			sc.recordProcessor.ProcessRecords(input)

			// Convert from nanoseconds to milliseconds
			processedRecordsTiming := time.Since(processRecordsStartTime) / 1000000
			sc.metrics.RecordProcessRecordsTime(shard.ID, float64(processedRecordsTiming))
		}

		sc.metrics.IncrRecordsProcessed(shard.ID, recordLength)
		sc.metrics.IncrBytesProcessed(shard.ID, recordBytes)
		sc.metrics.MillisBehindLatest(shard.ID, float64(*getResp.MillisBehindLatest))

		// Idle between each read, the user is responsible for checkpoint the progress
		// This value is only used when no records are returned; if records are returned, it should immediately
		// retrieve the next set of records.
		if recordLength == 0 && aws.Int64Value(getResp.MillisBehindLatest) < int64(sc.cfg.IdleTimeBetweenReadsInMillis) {
			time.Sleep(time.Duration(sc.cfg.IdleTimeBetweenReadsInMillis) * time.Millisecond)
		}

		// The shard has been closed, so no new records can be read from it
		if getResp.NextShardIterator == nil {
			log.Infof("Shard %s closed", shard.ID)
			shutdownInput := &ShutdownInput{ShutdownReason: TERMINATE, Checkpointer: recordCheckpointer}
			sc.recordProcessor.Shutdown(shutdownInput)
			return nil
		}
		shardIterator = getResp.NextShardIterator

		select {
		case <-*sc.stop:
			shutdownInput := &ShutdownInput{ShutdownReason: REQUESTED, Checkpointer: recordCheckpointer}
			sc.recordProcessor.Shutdown(shutdownInput)
			return nil
		default:
		}
	}
}

// Need to wait until the parent shard finished
func (sc *ShardConsumer) waitOnParentShard() error {
	shard := sc.shard
	if len(shard.ParentShardId) == 0 {
		return nil
	}

	pshard := &ShardStatus{
		ID:  shard.ParentShardId,
		mux: &sync.Mutex{},
	}

	for {
		if err := sc.checkpointer.FetchCheckpoint(pshard); err != nil {
			return err
		}

		// Parent shard is finished.
		if pshard.Checkpoint == SHARD_END {
			return nil
		}

		time.Sleep(time.Duration(sc.cfg.ParentShardPollIntervalMillis) * time.Millisecond)
	}
}

func (sc *ShardConsumer) getShardIterator(ctx context.Context) (*string, error) {
	shard := sc.shard
	log := sc.cfg.Logger

	// Get checkpoint of the shard from dynamoDB
	err := sc.checkpointer.FetchCheckpoint(shard)
	if err != nil && err != ErrSequenceIDNotFound {
		return nil, err
	}

	// If there isn't any checkpoint for the shard, use the configuration value.
	// TODO: configurable
	if shard.Checkpoint == "" {
		initPos := sc.cfg.InitialPositionInStream
		shardIteratorType := InitalPositionInStreamToShardIteratorType(initPos)
		log.Debugf("No checkpoint recorded for shard: %v, starting with: %v", shard.ID,
			aws.StringValue(shardIteratorType))

		var shardIterArgs *kinesis.GetShardIteratorInput
		if initPos == AT_TIMESTAMP {
			shardIterArgs = &kinesis.GetShardIteratorInput{
				ShardId:           &shard.ID,
				ShardIteratorType: shardIteratorType,
				Timestamp:         sc.cfg.InitialPositionInStreamExtended.Timestamp,
				StreamName:        &sc.streamName,
			}
		} else {
			shardIterArgs = &kinesis.GetShardIteratorInput{
				ShardId:           &shard.ID,
				ShardIteratorType: shardIteratorType,
				StreamName:        &sc.streamName,
			}
		}

		iterResp, err := sc.kc.GetShardIterator(shardIterArgs)
		if err != nil {
			return nil, err
		}
		return iterResp.ShardIterator, nil
	}

	log.Debugf("Start shard: %v at checkpoint: %v", shard.ID, shard.Checkpoint)
	shardIterArgs := &kinesis.GetShardIteratorInput{
		ShardId:                &shard.ID,
		ShardIteratorType:      aws.String("AFTER_SEQUENCE_NUMBER"),
		StartingSequenceNumber: &shard.Checkpoint,
		StreamName:             &sc.streamName,
	}
	iterResp, err := sc.kc.GetShardIterator(shardIterArgs)
	if err != nil {
		return nil, err
	}
	return iterResp.ShardIterator, nil
}

// Cleanup the internal lease cache
// Mutates shard { SetLeaseOwner }
func (sc *ShardConsumer) releaseLease() {
	log := sc.cfg.Logger
	shard := sc.shard
	log.Infof("Release lease for shard %s", shard.ID)
	shard.SetLeaseOwner("")

	// Release the lease by wiping out the lease owner for the shard
	// Note: we don't need to do anything in case of error here and shard lease will eventuall be expired.
	if err := sc.checkpointer.RemoveLeaseOwner(shard.ID); err != nil {
		log.Errorf("Failed to release shard lease or shard: %s Error: %+v", shard.ID, err)
	}

	// reporting lease lose metrics
	sc.metrics.LeaseLost(shard.ID)
}

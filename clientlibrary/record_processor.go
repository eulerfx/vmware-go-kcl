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
// The implementation is derived from https://github.com/awslabs/amazon-kinesis-client
/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package kcl

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	ks "github.com/aws/aws-sdk-go/service/kinesis"
)

const (
	LEASE_KEY_KEY                  = "ShardID"
	LEASE_OWNER_KEY                = "AssignedTo"
	LEASE_TIMEOUT_KEY              = "LeaseTimeout"
	CHECKPOINT_SEQUENCE_NUMBER_KEY = "Checkpoint"
	PARENT_SHARD_ID_KEY            = "ParentShardId"

	// We've completely processed all records in this shard.
	SHARD_END = "SHARD_END"

	// ErrLeaseNotAquired is returned when we failed to get a lock on the shard
	ErrLeaseNotAquired = "Lease is already held by another node"
)

const (
	/**
	 * Indicates that the entire application is being shutdown, and if desired the record processor will be given a
	 * final chance to checkpoint. This state will not trigger a direct call to
	 * {@link com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor#shutdown(ShutdownInput)}, but
	 * instead depend on a different interface for backward compatibility.
	 */
	REQUESTED ShutdownReason = iota + 1
	/**
	 * Terminate processing for this RecordProcessor (resharding use case).
	 * Indicates that the shard is closed and all records from the shard have been delivered to the application.
	 * Applications SHOULD checkpoint their progress to indicate that they have successfully processed all records
	 * from this shard and processing of child shards can be started.
	 */
	TERMINATE
	/**
	 * Processing will be moved to a different record processor (fail over, load balancing use cases).
	 * Applications SHOULD NOT checkpoint their progress (as another record processor may have already started
	 * processing data).
	 */
	ZOMBIE
)

// Checkpointer handles checkpointing when a record has been processed
type Checkpointer interface {
	// Init initialises the Checkpoint
	Init() error

	// GetLease attempts to gain a lock on the given shard
	// Mutates ShardStatus.
	GetLease(context.Context, *ShardStatus, string) error

	// CheckpointSequence writes a checkpoint at the designated sequence ID
	// Does not mutate ShardStatus.
	CheckpointSequence(*ShardStatus) error

	// FetchCheckpoint retrieves the checkpoint for the given shard
	// Mutates ShardStatus.
	FetchCheckpoint(*ShardStatus) error

	// RemoveLeaseInfo to remove lease info for shard entry because the shard no longer exists
	RemoveLeaseInfo(string) error

	// RemoveLeaseOwner to remove lease owner for the shard entry to make the shard available for reassignment
	RemoveLeaseOwner(string) error
}

// ErrSequenceIDNotFound is returned by FetchCheckpoint when no SequenceID is found
var ErrSequenceIDNotFound = errors.New("SequenceIDNotFoundForShard")

// Containers for the parameters to the IRecordProcessor

/**
 * Reason the RecordProcessor is being shutdown.
 * Used to distinguish between a fail-over vs. a termination (shard is closed and all records have been delivered).
 * In case of a fail over, applications should NOT checkpoint as part of shutdown,
 * since another record processor may have already started processing records for that shard.
 * In case of termination (resharding use case), applications SHOULD checkpoint their progress to indicate
 * that they have successfully processed all the records (processing of child shards can then begin).
 */
type ShutdownReason int

type InitializationInput struct {
	ShardID                         string
	ExtendedSequenceNumber          *ExtendedSequenceNumber
	PendingCheckpointSequenceNumber *ExtendedSequenceNumber
}

type ProcessRecordsInput struct {
	CacheEntryTime     *time.Time
	CacheExitTime      *time.Time
	Records            []*ks.Record
	Checkpointer       *RecordProcessorCheckpointer
	MillisBehindLatest int64
}

type ShutdownInput struct {
	ShutdownReason ShutdownReason
	Checkpointer   *RecordProcessorCheckpointer
}

var shutdownReasonMap = map[ShutdownReason]*string{
	REQUESTED: aws.String("REQUESTED"),
	TERMINATE: aws.String("TERMINATE"),
	ZOMBIE:    aws.String("ZOMBIE"),
}

func ShutdownReasonMessage(reason ShutdownReason) *string {
	return shutdownReasonMap[reason]
}

func newInitialPositionAtTimestamp(timestamp *time.Time) *InitialPositionInStreamExtended {
	return &InitialPositionInStreamExtended{Position: AT_TIMESTAMP, Timestamp: timestamp}
}

func newInitialPosition(position InitialPositionInStream) *InitialPositionInStreamExtended {
	return &InitialPositionInStreamExtended{Position: position, Timestamp: nil}
}

/**
 * Used by RecordProcessors when they want to checkpoint their progress.
 * The Kinesis Client Library will pass an object implementing this interface to RecordProcessors, so they can
 * checkpoint their progress.
 */
type RecordProcessorCheckpointer struct {
	shard      *ShardStatus
	checkpoint Checkpointer
}

func NewRecordProcessorCheckpointer(shard *ShardStatus, checkpoint Checkpointer) *RecordProcessorCheckpointer {
	return &RecordProcessorCheckpointer{
		shard:      shard,
		checkpoint: checkpoint,
	}
}

// Checkpoint records a checkpoint at the specified SN.
// Mutates ShardStatus.Checkpoint = { SN }, calls checkpoint store

// 	/**
// 	 * This method will checkpoint the progress at the provided sequenceNumber. This method is analogous to
// 	 * {@link #checkpoint()} but provides the ability to specify the sequence number at which to
// 	 * checkpoint.
// 	 *
// 	 * @param sequenceNumber A sequence number at which to checkpoint in this shard. Upon failover,
// 	 *        the Kinesis Client Library will start fetching records after this sequence number.
// 	 * @error ThrottlingError Can't store checkpoint. Can be caused by checkpointing too frequently.
// 	 *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
// 	 * @error ShutdownError The record processor instance has been shutdown. Another instance may have
// 	 *         started processing some of these records already.
// 	 *         The application should abort processing via this RecordProcessor instance.
// 	 * @error InvalidStateError Can't store checkpoint.
// 	 *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
// 	 * @error KinesisClientLibDependencyError Encountered an issue when storing the checkpoint. The application can
// 	 *         backoff and retry.
// 	 * @error IllegalArgumentError The sequence number is invalid for one of the following reasons:
// 	 *         1.) It appears to be out of range, i.e. it is smaller than the last check point value, or larger than the
// 	 *         greatest sequence number seen by the associated record processor.
// 	 *         2.) It is not a valid sequence number for a record in this shard.
// 	 */
func (rc *RecordProcessorCheckpointer) Checkpoint(sequenceNumber *string) error {
	rc.shard.mux.Lock()

	// checkpoint the last sequence of a closed shard
	if sequenceNumber == nil {
		rc.shard.Checkpoint = SHARD_END
	} else {
		rc.shard.Checkpoint = aws.StringValue(sequenceNumber)
	}

	// TODO: defer?
	rc.shard.mux.Unlock()

	// DOES NOT mutate shard
	err := rc.checkpoint.CheckpointSequence(rc.shard)
	if err != nil {
		return err
	}

	return nil
}

// 	/**
// 	 * This method will record a pending checkpoint at the provided sequenceNumber.
// 	 *
// 	 * @param sequenceNumber A sequence number at which to prepare checkpoint in this shard.

// 	 * @return an IPreparedCheckpointer object that can be called later to persist the checkpoint.
// 	 *
// 	 * @error ThrottlingError Can't store pending checkpoint. Can be caused by checkpointing too frequently.
// 	 *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
// 	 * @error ShutdownError The record processor instance has been shutdown. Another instance may have
// 	 *         started processing some of these records already.
// 	 *         The application should abort processing via this RecordProcessor instance.
// 	 * @error InvalidStateError Can't store pending checkpoint.
// 	 *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
// 	 * @error KinesisClientLibDependencyError Encountered an issue when storing the pending checkpoint. The
// 	 *         application can backoff and retry.
// 	 * @error IllegalArgumentError The sequence number is invalid for one of the following reasons:
// 	 *         1.) It appears to be out of range, i.e. it is smaller than the last check point value, or larger than the
// 	 *         greatest sequence number seen by the associated record processor.
// 	 *         2.) It is not a valid sequence number for a record in this shard.
// 	 */
func (rc *RecordProcessorCheckpointer) PrepareCheckpoint(sequenceNumber *string) (func() error, error) {
	commit := func() error {
		rc.Checkpoint(sequenceNumber)
		return nil
	}
	return commit, nil

}

// RecordProcessor is the interface for some callback functions invoked by KCL will
// The main task of using KCL is to provide implementation on RecordProcessor interface.
// Note: This is exactly the same interface as Amazon KCL RecordProcessor v2
type RecordProcessor interface {
	/**
	 * Invoked by the Amazon Kinesis Client Library before data records are delivered to the RecordProcessor instance
	 * (via processRecords).
	 *
	 * @param initializationInput Provides information related to initialization
	 */
	Initialize(initializationInput *InitializationInput)

	/**
	 * Process data records. The Amazon Kinesis Client Library will invoke this method to deliver data records to the
	 * application.
	 * Upon fail over, the new instance will get records with sequence number > checkpoint position
	 * for each partition key.
	 *
	 * @param processRecordsInput Provides the records to be processed as well as information and capabilities related
	 *        to them (eg checkpointing).
	 */
	ProcessRecords(processRecordsInput *ProcessRecordsInput)

	/**
	 * Invoked by the Amazon Kinesis Client Library to indicate it will no longer send data records to this
	 * RecordProcessor instance.
	 *
	 * <h2><b>Warning</b></h2>
	 *
	 * When the value of {@link ShutdownInput#getShutdownReason()} is
	 * {@link com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason#TERMINATE} it is required that you
	 * checkpoint. Failure to do so will result in an IllegalArgumentException, and the KCL no longer making progress.
	 *
	 * @param shutdownInput
	 *            Provides information and capabilities (eg checkpointing) related to shutdown of this record processor.
	 */
	Shutdown(shutdownInput *ShutdownInput)
}

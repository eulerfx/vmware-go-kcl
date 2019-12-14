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
package kcl

import (
	"github.com/aws/aws-sdk-go/aws"

	//chk "github.com/vmware/vmware-go-kcl/clientlibrary/checkpoint"
	//kcl "github.com/vmware/vmware-go-kcl/clientlibrary/interfaces"
	//par "github.com/vmware/vmware-go-kcl/clientlibrary/partition"
)

type (
	IPreparedCheckpointer interface {
		GetPendingCheckpoint() *ExtendedSequenceNumber

		/**
		 * This method will record a pending checkpoint.
		 *
		 * @error ThrottlingError Can't store checkpoint. Can be caused by checkpointing too frequently.
		 *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
		 * @error ShutdownError The record processor instance has been shutdown. Another instance may have
		 *         started processing some of these records already.
		 *         The application should abort processing via this RecordProcessor instance.
		 * @error InvalidStateError Can't store checkpoint.
		 *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
		 * @error KinesisClientLibDependencyError Encountered an issue when storing the checkpoint. The application can
		 *         backoff and retry.
		 * @error IllegalArgumentError The sequence number being checkpointed is invalid because it is out of range,
		 *         i.e. it is smaller than the last check point value (prepared or committed), or larger than the greatest
		 *         sequence number seen by the associated record processor.
		 */
		Checkpoint() error
	}

	/**
	 * Used by RecordProcessors when they want to checkpoint their progress.
	 * The Kinesis Client Library will pass an object implementing this interface to RecordProcessors, so they can
	 * checkpoint their progress.
	 */
	IRecordProcessorCheckpointer interface {
		/**
		 * This method will checkpoint the progress at the provided sequenceNumber. This method is analogous to
		 * {@link #checkpoint()} but provides the ability to specify the sequence number at which to
		 * checkpoint.
		 *
		 * @param sequenceNumber A sequence number at which to checkpoint in this shard. Upon failover,
		 *        the Kinesis Client Library will start fetching records after this sequence number.
		 * @error ThrottlingError Can't store checkpoint. Can be caused by checkpointing too frequently.
		 *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
		 * @error ShutdownError The record processor instance has been shutdown. Another instance may have
		 *         started processing some of these records already.
		 *         The application should abort processing via this RecordProcessor instance.
		 * @error InvalidStateError Can't store checkpoint.
		 *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
		 * @error KinesisClientLibDependencyError Encountered an issue when storing the checkpoint. The application can
		 *         backoff and retry.
		 * @error IllegalArgumentError The sequence number is invalid for one of the following reasons:
		 *         1.) It appears to be out of range, i.e. it is smaller than the last check point value, or larger than the
		 *         greatest sequence number seen by the associated record processor.
		 *         2.) It is not a valid sequence number for a record in this shard.
		 */
		Checkpoint(sequenceNumber *string) error

		/**
		 * This method will record a pending checkpoint at the provided sequenceNumber.
		 *
		 * @param sequenceNumber A sequence number at which to prepare checkpoint in this shard.

		 * @return an IPreparedCheckpointer object that can be called later to persist the checkpoint.
		 *
		 * @error ThrottlingError Can't store pending checkpoint. Can be caused by checkpointing too frequently.
		 *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
		 * @error ShutdownError The record processor instance has been shutdown. Another instance may have
		 *         started processing some of these records already.
		 *         The application should abort processing via this RecordProcessor instance.
		 * @error InvalidStateError Can't store pending checkpoint.
		 *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
		 * @error KinesisClientLibDependencyError Encountered an issue when storing the pending checkpoint. The
		 *         application can backoff and retry.
		 * @error IllegalArgumentError The sequence number is invalid for one of the following reasons:
		 *         1.) It appears to be out of range, i.e. it is smaller than the last check point value, or larger than the
		 *         greatest sequence number seen by the associated record processor.
		 *         2.) It is not a valid sequence number for a record in this shard.
		 */
		PrepareCheckpoint(sequenceNumber *string) (IPreparedCheckpointer, error)
	}
)


type (

	/* Objects of this class are prepared to checkpoint at a specific sequence number. They use an
	 * IRecordProcessorCheckpointer to do the actual checkpointing, so their checkpoint is subject to the same 'didn't go
	 * backwards' validation as a normal checkpoint.
	 */
	PreparedCheckpointer struct {
		pendingCheckpointSequenceNumber *ExtendedSequenceNumber
		checkpointer                    IRecordProcessorCheckpointer
	}

	/**
	 * This class is used to enable RecordProcessors to checkpoint their progress.
	 * The Amazon Kinesis Client Library will instantiate an object and provide a reference to the application
	 * RecordProcessor instance. Amazon Kinesis Client Library will create one instance per shard assignment.
	 */
	RecordProcessorCheckpointer struct {
		shard      *ShardStatus
		checkpoint Checkpointer
	}
)

func NewRecordProcessorCheckpoint(shard *ShardStatus, checkpoint Checkpointer) IRecordProcessorCheckpointer {
	return &RecordProcessorCheckpointer{
		shard:      shard,
		checkpoint: checkpoint,
	}
}

func (pc *PreparedCheckpointer) GetPendingCheckpoint() *ExtendedSequenceNumber {
	return pc.pendingCheckpointSequenceNumber
}

func (pc *PreparedCheckpointer) Checkpoint() error {
	return pc.checkpointer.Checkpoint(pc.pendingCheckpointSequenceNumber.SequenceNumber)
}

func (rc *RecordProcessorCheckpointer) Checkpoint(sequenceNumber *string) error {
	rc.shard.Mux.Lock()

	// checkpoint the last sequence of a closed shard
	if sequenceNumber == nil {
		rc.shard.Checkpoint = SHARD_END
	} else {
		rc.shard.Checkpoint = aws.StringValue(sequenceNumber)
	}

	rc.shard.Mux.Unlock()
	return rc.checkpoint.CheckpointSequence(rc.shard)
}

func (rc *RecordProcessorCheckpointer) PrepareCheckpoint(sequenceNumber *string) (IPreparedCheckpointer, error) {
	return &PreparedCheckpointer{}, nil

}

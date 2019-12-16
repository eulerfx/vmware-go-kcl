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
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

// Consumer is the high level class that Kinesis applications use to start processing data. It initializes and oversees
// different components (e.g. syncing shard and lease information, tracking shard assignments, and processing data from
// the shards)
type Consumer struct {
	streamName string
	regionName string
	workerID   string

	makeProcessor func() RecordProcessor
	cfg           *ConsumerConfig
	kc            kinesisiface.KinesisAPI
	checkpointer  Checkpointer
	metrics       MonitoringService

	stop                   *chan struct{}
	shardConsumerWaitGroup *sync.WaitGroup // for shard consumers
	done                   bool

	// for jitter
	rng *rand.Rand

	// keyed by Shard.ID.
	shardStatus map[string]*ShardStatus
}

// NewConsumer constructs a Worker instance for processing Kinesis stream data.
func NewConsumer(factory func() RecordProcessor, cfg *ConsumerConfig) *Consumer {
	mService := cfg.MonitoringService
	if mService == nil {
		// Replaces nil with noop monitor service (not emitting any metrics).
		mService = NoopMonitoringService{}
	}

	// Create a pseudo-random number generator and seed it.
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	return &Consumer{
		streamName:    cfg.StreamName,
		regionName:    cfg.RegionName,
		workerID:      cfg.WorkerID,
		makeProcessor: factory,
		cfg:           cfg,
		metrics:       mService,
		done:          false,
		rng:           rng,
	}
}

// WithKinesis is used to provide Kinesis service for either custom implementation or unit testing.
func (c *Consumer) WithKinesis(svc kinesisiface.KinesisAPI) *Consumer {
	c.kc = svc
	return c
}

// WithCheckpointer is used to provide a custom checkpointer service for non-dynamodb implementation
// or unit testing.
func (c *Consumer) WithCheckpointer(checker Checkpointer) *Consumer {
	c.checkpointer = checker
	return c
}

// Start starts consuming data from the stream, and pass it to the application record processors.
func (c *Consumer) Start(ctx context.Context) error {
	log := c.cfg.Log
	if err := c.init(); err != nil {
		log.Errorf("Failed to initialize Worker: %+v", err)
		return err
	}

	// Start monitoring service
	log.Infof("Starting monitoring service.")
	if err := c.metrics.Start(); err != nil {
		log.Errorf("Failed to start monitoring service: %+v", err)
		return err
	}

	log.Infof("Starting worker event loop.")
	c.shardConsumerWaitGroup.Add(1)
	go func() {
		defer c.shardConsumerWaitGroup.Done()
		// entering event loop
		c.eventLoop(ctx)
	}()

	return nil
}

// Shutdown signals worker to shutdown. Worker will try initiating shutdown of all record processors.
func (c *Consumer) Shutdown() {
	log := c.cfg.Log
	log.Infof("Worker shutdown in requested.")

	if c.done {
		return
	}

	close(*c.stop)
	c.done = true
	c.shardConsumerWaitGroup.Wait() // shard consumers

	c.metrics.Shutdown()
	log.Infof("Worker loop is complete. Exiting from worker.")
}

// Publish to write some data into stream. This function is mainly used for testing purpose.
func (c *Consumer) Publish(streamName, partitionKey string, data []byte) error {
	log := c.cfg.Log
	_, err := c.kc.PutRecord(&kinesis.PutRecordInput{
		Data:         data,
		StreamName:   aws.String(streamName),
		PartitionKey: aws.String(partitionKey),
	})
	if err != nil {
		log.Errorf("Error in publishing data to %s/%s. Error: %+v", streamName, partitionKey, err)
	}
	return err
}

// init
func (c *Consumer) init() error {
	log := c.cfg.Log
	log.Infof("Worker initialization in progress...")

	// Create default Kinesis session
	if c.kc == nil {
		// create session for Kinesis
		log.Infof("Creating Kinesis session")

		s, err := session.NewSession(&aws.Config{
			Region:      aws.String(c.regionName),
			Endpoint:    &c.cfg.KinesisEndpoint,
			Credentials: c.cfg.KinesisCredentials,
		})

		if err != nil {
			// no need to move forward
			log.Fatalf("Failed in getting Kinesis session for creating Worker: %+v", err)
		}
		c.kc = kinesis.New(s)
	} else {
		log.Infof("Use custom Kinesis service.")
	}

	// // Create default dynamodb based checkpointer implementation
	// if c.checkpointer == nil {
	// 	log.Infof("Creating DynamoDB based checkpointer")
	// 	cp := db.NewCheckpointer(c.cfg)
	// 	c.checkpointer = cp
	// } else {
	// 	log.Infof("Use custom checkpointer implementation.")
	// }

	err := c.metrics.Init(c.cfg.ApplicationName, c.streamName, c.workerID)
	if err != nil {
		log.Errorf("Failed to start monitoring service: %+v", err)
	}

	log.Infof("Initializing Checkpointer")
	if err := c.checkpointer.Init(); err != nil {
		log.Errorf("Failed to start Checkpointer: %+v", err)
		return err
	}

	c.shardStatus = make(map[string]*ShardStatus)

	stopChan := make(chan struct{})
	c.stop = &stopChan

	c.shardConsumerWaitGroup = &sync.WaitGroup{}

	log.Infof("Initialization complete.")

	return nil
}

// newShardConsumer to create a shard consumer instance
func (c *Consumer) newShardConsumer(shard *ShardStatus) *ShardConsumer {
	return &ShardConsumer{
		streamName:      c.streamName,
		shard:           shard,
		kc:              c.kc,
		checkpointer:    c.checkpointer,
		recordProcessor: c.makeProcessor(),
		cfg:             c.cfg,
		consumerID:      c.workerID,
		stop:            c.stop,
		metrics:         c.metrics,
		//state:           WAITING_ON_PARENT_SHARDS,
	}
}

// eventLoop
func (c *Consumer) eventLoop(ctx context.Context) {
	log := c.cfg.Log
	// Add [-50%, +50%] random jitter to ShardSyncIntervalMillis. When multiple workers
	// starts at the same time, this decreases the probability of them calling
	// kinesis.DescribeStream at the same time, and hit the hard-limit on aws API calls.
	// On average the period remains the same so that doesn't affect behavior.
	shardSyncSleep := c.cfg.ShardSyncIntervalMillis/2 + c.rng.Intn(int(c.cfg.ShardSyncIntervalMillis))
	// each tick: { sync shard status, for each shard: { fetch checkpoint, acquire lease, and starts a shard consumer } }
	for {
		err := c.syncShard(ctx)
		if err != nil {
			log.Errorf("Error syncing shards: %+v, Retrying in %d ms...", err, shardSyncSleep)
			time.Sleep(time.Duration(shardSyncSleep) * time.Millisecond)
			continue
		}

		log.Infof("Found %d shards", len(c.shardStatus))

		// Count the number of leases hold by this worker excluding the processed shard
		counter := 0
		for _, shard := range c.shardStatus {
			if shard.GetLeaseOwner() == c.workerID && shard.Checkpoint != SHARD_END {
				counter++
			}
		}

		// max number of lease has not been reached yet
		if counter >= c.cfg.MaxLeasesForWorker {
			// yes, its a goto, yes it can be refactored
			goto WAIT_OR_STOP
		}

		for _, shard := range c.shardStatus {
			// already owner of the shard
			if shard.GetLeaseOwner() == c.workerID {
				continue
			}

			// per shard consumer context.
			ctx := context.Background()

			err := c.checkpointer.FetchCheckpoint(ctx, shard)
			if err != nil {
				// checkpoint may not existed yet is not an error condition.
				if err != ErrSequenceIDNotFound {
					log.Errorf(" Error: %+v", err)
					// move on to next shard
					continue
				}
			}

			// The shard is closed and we have processed all records
			if shard.Checkpoint == SHARD_END {
				continue
			}

			err = c.checkpointer.GetLease(ctx, shard, c.workerID)
			if err != nil {
				// cannot get lease on the shard
				if err.Error() != ErrLeaseNotAquired {
					log.Errorf("Cannot get lease: %+v", err)
				}
				continue
			}

			// log metrics on got lease
			c.metrics.LeaseGained(shard.ID)

			log.Infof("Start Shard Consumer for shard: %v", shard.ID)

			shardConsumer := c.newShardConsumer(shard)
			c.shardConsumerWaitGroup.Add(1)
			ctx, cancel := context.WithCancel(ctx)
			go func() {
				//ctx, cancel := context.WithCancel(ctx)
				defer c.shardConsumerWaitGroup.Done()
				defer cancel()
				if err := shardConsumer.run(ctx); err != nil {
					log.Errorf("Error in getRecords: %+v", err)
				}
			}()
			// exit from for loop and not to grab more shard for now.
			break
		}

	WAIT_OR_STOP:
		select {
		case <-*c.stop:
			log.Infof("Shutting down...")
			return
		case <-time.After(time.Duration(shardSyncSleep) * time.Millisecond):
			log.Debugf("Waited %d ms to sync shards...", shardSyncSleep)
		}
	}
}

// List all ACTIVE shard and store them into shardStatus table
// If shard has been removed, need to exclude it from cached shard status.
func (c *Consumer) getShardIDs(startShardID string, shardInfo map[string]bool) error {
	log := c.cfg.Log
	// The default pagination limit is 100.
	args := &kinesis.DescribeStreamInput{
		StreamName: aws.String(c.streamName),
	}

	if startShardID != "" {
		args.ExclusiveStartShardId = aws.String(startShardID)
	}

	streamDesc, err := c.kc.DescribeStream(args)
	if err != nil {
		log.Errorf("Error in DescribeStream: %s Error: %+v Request: %s", c.streamName, err, args)
		return err
	}

	if *streamDesc.StreamDescription.StreamStatus != "ACTIVE" {
		log.Warnf("Stream %s is not active", c.streamName)
		return errors.New("stream not active")
	}

	var lastShardID string
	for _, s := range streamDesc.StreamDescription.Shards {
		// record avail shardId from fresh reading from Kinesis
		shardInfo[*s.ShardId] = true

		// found new shard
		if _, ok := c.shardStatus[*s.ShardId]; !ok {
			log.Infof("Found new shard with id %s", *s.ShardId)
			c.shardStatus[*s.ShardId] = &ShardStatus{
				ID:                     *s.ShardId,
				ParentShardId:          aws.StringValue(s.ParentShardId),
				mux:                    &sync.Mutex{},
				StartingSequenceNumber: aws.StringValue(s.SequenceNumberRange.StartingSequenceNumber),
				EndingSequenceNumber:   aws.StringValue(s.SequenceNumberRange.EndingSequenceNumber),
			}
		}
		lastShardID = *s.ShardId
	}

	if *streamDesc.StreamDescription.HasMoreShards {
		err := c.getShardIDs(lastShardID, shardInfo)
		if err != nil {
			log.Errorf("Error in getShardIDs: %s Error: %+v", lastShardID, err)
			return err
		}
	}

	return nil
}

// syncShard to sync the cached shard info with actual shard info from Kinesis
func (c *Consumer) syncShard(ctx context.Context) error {
	log := c.cfg.Log
	shardInfo := make(map[string]bool)
	err := c.getShardIDs("", shardInfo)
	if err != nil {
		return err
	}

	for _, shard := range c.shardStatus {
		// The cached shard no longer existed, remove it.
		if _, ok := shardInfo[shard.ID]; !ok {
			// remove the shard from local status cache
			// TODO: move after RemoveLeaseInfo?
			delete(c.shardStatus, shard.ID)
			// remove the shard entry in dynamoDB as well
			// Note: syncShard runs periodically. we don't need to do anything in case of error here.
			if err := c.checkpointer.RemoveLeaseInfo(ctx, shard.ID); err != nil {
				log.Errorf("Failed to remove shard lease info: %s Error: %+v", shard.ID, err)
			}
		}
	}

	return nil
}

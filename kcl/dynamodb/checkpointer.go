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
package dynamodb

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"

	"github.com/vmware/vmware-go-kcl/kcl"
	"github.com/vmware/vmware-go-kcl/logger"
)

const (
	LEASE_KEY_KEY                  = "ShardID"
	LEASE_OWNER_KEY                = "AssignedTo"
	LEASE_TIMEOUT_KEY              = "LeaseTimeout"
	CHECKPOINT_SEQUENCE_NUMBER_KEY = "Checkpoint"
	PARENT_SHARD_ID_KEY            = "ParentShardId"

	// ErrInvalidDynamoDBSchema is returned when there are one or more fields missing from the table
	ErrInvalidDynamoDBSchema = "The DynamoDB schema is invalid and may need to be re-created"

	// NumMaxRetries is the max times of doing retry
	NumMaxRetries = 5
)

// DynamoDbCheckpointer implements the Checkpoint interface using DynamoDB as a backend.
type Checkpointer struct {
	log                     logger.Logger
	TableName               string
	leaseTableReadCapacity  int64
	leaseTableWriteCapacity int64
	LeaseDuration           int
	svc                     dynamodbiface.DynamoDBAPI
	kclConfig               *kcl.ConsumerConfig
	Retries                 int
}

// NewCheckpointer creates a new checkpointer.
func NewCheckpointer(cfg *kcl.ConsumerConfig) *Checkpointer {
	checkpointer := &Checkpointer{
		log:                     cfg.Log,
		TableName:               cfg.TableName,
		leaseTableReadCapacity:  int64(cfg.InitialLeaseTableReadCapacity),
		leaseTableWriteCapacity: int64(cfg.InitialLeaseTableWriteCapacity),
		LeaseDuration:           cfg.FailoverTimeMillis,
		kclConfig:               cfg,
		Retries:                 NumMaxRetries,
	}
	return checkpointer
}

// WithDynamoDB is used to provide DynamoDB service
func (cp *Checkpointer) WithDynamoDB(svc dynamodbiface.DynamoDBAPI) *Checkpointer {
	cp.svc = svc
	return cp
}

// Init initialises the DynamoDB Checkpointer
func (cp *Checkpointer) Init() error {
	cp.log.Infof("Creating DynamoDB session")
	s, err := session.NewSession(&aws.Config{
		Region:      aws.String(cp.kclConfig.RegionName),
		Endpoint:    aws.String(cp.kclConfig.DynamoDBEndpoint),
		Credentials: cp.kclConfig.DynamoDBCredentials,
		Retryer:     client.DefaultRetryer{NumMaxRetries: cp.Retries},
	})
	if err != nil {
		// no need to move forward
		cp.log.Fatalf("Failed in getting DynamoDB session for creating Worker: %+v", err)
	}

	if cp.svc == nil {
		cp.svc = dynamodb.New(s)
	}

	if !cp.doesTableExist() {
		return cp.createTable()
	}
	return nil
}

// GetLease attempts to gain a lock on the given shard
// Mutates { shard.AssignedTo, LeaseExpires }
func (cp *Checkpointer) GetLease(ctx context.Context, shard *kcl.ShardStatus, newAssignTo string) error {
	newLeaseTimeout := time.Now().Add(time.Duration(cp.LeaseDuration) * time.Millisecond).UTC()
	newLeaseTimeoutString := newLeaseTimeout.Format(time.RFC3339)
	currentCheckpoint, err := cp.getItem(shard.ID)
	if err != nil {
		return err
	}

	assignedVar, assignedToOk := currentCheckpoint[LEASE_OWNER_KEY]
	leaseVar, leaseTimeoutOk := currentCheckpoint[LEASE_TIMEOUT_KEY]
	var conditionalExpression string
	var expressionAttributeValues map[string]*dynamodb.AttributeValue

	if !leaseTimeoutOk || !assignedToOk {
		conditionalExpression = "attribute_not_exists(AssignedTo)"
	} else {
		assignedTo := *assignedVar.S
		leaseTimeout := *leaseVar.S

		currentLeaseTimeout, err := time.Parse(time.RFC3339, leaseTimeout)
		if err != nil {
			return err
		}

		if time.Now().UTC().Before(currentLeaseTimeout) && assignedTo != newAssignTo {
			return errors.New(kcl.ErrLeaseNotAquired)
		}

		cp.log.Debugf("Attempting to get a lock for shard: %s, leaseTimeout: %s, assignedTo: %s", shard.ID, currentLeaseTimeout, assignedTo)
		conditionalExpression = "ShardID = :id AND AssignedTo = :assigned_to AND LeaseTimeout = :lease_timeout"
		expressionAttributeValues = map[string]*dynamodb.AttributeValue{
			":id": {
				S: aws.String(shard.ID),
			},
			":assigned_to": {
				S: aws.String(assignedTo),
			},
			":lease_timeout": {
				S: aws.String(leaseTimeout),
			},
		}
	}

	marshalledCheckpoint := map[string]*dynamodb.AttributeValue{
		LEASE_KEY_KEY: {
			S: aws.String(shard.ID),
		},
		LEASE_OWNER_KEY: {
			S: aws.String(newAssignTo),
		},
		LEASE_TIMEOUT_KEY: {
			S: aws.String(newLeaseTimeoutString),
		},
	}

	if len(shard.ParentShardId) > 0 {
		marshalledCheckpoint[PARENT_SHARD_ID_KEY] = &dynamodb.AttributeValue{S: aws.String(shard.ParentShardId)}
	}

	if shard.Checkpoint != "" {
		marshalledCheckpoint[CHECKPOINT_SEQUENCE_NUMBER_KEY] = &dynamodb.AttributeValue{
			S: aws.String(shard.Checkpoint),
		}
	}

	err = cp.conditionalUpdate(conditionalExpression, expressionAttributeValues, marshalledCheckpoint)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return errors.New(kcl.ErrLeaseNotAquired)
			}
		}
		return err
	}

	shard.Mux.Lock()
	shard.AssignedTo = newAssignTo
	shard.LeaseTimeout = newLeaseTimeout
	shard.Mux.Unlock()

	return nil
}

// CheckpointSequence writes a checkpoint at the designated sequence ID
// Does NOT mutate shard.
func (cp *Checkpointer) CheckpointSequence(ctx context.Context, shard *kcl.ShardStatus) error {
	leaseTimeout := shard.LeaseTimeout.UTC().Format(time.RFC3339)
	marshalledCheckpoint := map[string]*dynamodb.AttributeValue{
		LEASE_KEY_KEY: {
			S: aws.String(shard.ID),
		},
		CHECKPOINT_SEQUENCE_NUMBER_KEY: {
			S: aws.String(shard.Checkpoint),
		},
		LEASE_OWNER_KEY: {
			S: aws.String(shard.AssignedTo),
		},
		LEASE_TIMEOUT_KEY: {
			S: aws.String(leaseTimeout),
		},
	}

	if len(shard.ParentShardId) > 0 {
		marshalledCheckpoint[PARENT_SHARD_ID_KEY] = &dynamodb.AttributeValue{S: &shard.ParentShardId}
	}

	return cp.saveItem(marshalledCheckpoint)
}

// FetchCheckpoint retrieves the checkpoint for the given shard
// Mutates { shard.Checkpoint, shard.AssignedTo }
func (cp *Checkpointer) FetchCheckpoint(ctx context.Context, shard *kcl.ShardStatus) error {
	row, err := cp.getItem(shard.ID)
	if err != nil {
		return err
	}

	sequenceID, ok := row[CHECKPOINT_SEQUENCE_NUMBER_KEY]
	if !ok {
		return kcl.ErrSequenceIDNotFound
	}
	cp.log.Debugf("Retrieved Shard Iterator %s", *sequenceID.S)
	shard.Mux.Lock()
	defer shard.Mux.Unlock()
	shard.Checkpoint = aws.StringValue(sequenceID.S)

	if assignedTo, ok := row[LEASE_OWNER_KEY]; ok {
		shard.AssignedTo = aws.StringValue(assignedTo.S)
	}
	return nil
}

// RemoveLeaseInfo to remove lease info for shard entry in dynamoDB because the shard no longer exists in Kinesis
func (cp *Checkpointer) RemoveLeaseInfo(ctx context.Context, shardID string) error {
	err := cp.removeItem(shardID)
	if err != nil {
		cp.log.Errorf("Error in removing lease info for shard: %s, Error: %+v", shardID, err)
	} else {
		cp.log.Infof("Lease info for shard: %s has been removed.", shardID)
	}
	return err
}

// RemoveLeaseOwner to remove lease owner for the shard entry
func (cp *Checkpointer) RemoveLeaseOwner(ctx context.Context, shardID string) error {
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(cp.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			LEASE_KEY_KEY: {
				S: aws.String(shardID),
			},
		},
		UpdateExpression: aws.String("remove " + LEASE_OWNER_KEY),
	}

	_, err := cp.svc.UpdateItem(input)

	return err
}

func (cp *Checkpointer) createTable() error {
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(LEASE_KEY_KEY),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(LEASE_KEY_KEY),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(cp.leaseTableReadCapacity),
			WriteCapacityUnits: aws.Int64(cp.leaseTableWriteCapacity),
		},
		TableName: aws.String(cp.TableName),
	}
	_, err := cp.svc.CreateTable(input)
	return err
}

func (cp *Checkpointer) doesTableExist() bool {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(cp.TableName),
	}
	_, err := cp.svc.DescribeTable(input)
	return err == nil
}

func (cp *Checkpointer) saveItem(item map[string]*dynamodb.AttributeValue) error {
	return cp.putItem(&dynamodb.PutItemInput{
		TableName: aws.String(cp.TableName),
		Item:      item,
	})
}

func (cp *Checkpointer) conditionalUpdate(conditionExpression string, expressionAttributeValues map[string]*dynamodb.AttributeValue,
	item map[string]*dynamodb.AttributeValue) error {
	return cp.putItem(&dynamodb.PutItemInput{
		ConditionExpression:       aws.String(conditionExpression),
		TableName:                 aws.String(cp.TableName),
		Item:                      item,
		ExpressionAttributeValues: expressionAttributeValues,
	})
}

func (cp *Checkpointer) putItem(input *dynamodb.PutItemInput) error {
	_, err := cp.svc.PutItem(input)
	return err
}

func (cp *Checkpointer) getItem(shardID string) (map[string]*dynamodb.AttributeValue, error) {
	item, err := cp.svc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(cp.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			LEASE_KEY_KEY: {
				S: aws.String(shardID),
			},
		},
	})
	return item.Item, err
}

func (cp *Checkpointer) removeItem(shardID string) error {
	_, err := cp.svc.DeleteItem(&dynamodb.DeleteItemInput{
		TableName: aws.String(cp.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			LEASE_KEY_KEY: {
				S: aws.String(shardID),
			},
		},
	})
	return err
}

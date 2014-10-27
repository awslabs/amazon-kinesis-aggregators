/**
 * Amazon Kinesis Aggregators
 *
 * Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.aggregators;

import java.util.List;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.model.Record;

/**
 * Stream aggregators give end users the ability to dynamically aggregate
 * Kinesis data in Dynamo DB. All a consumer must do is create an Aggregator and
 * then call aggregate within the processRecords method of an IRecordProcessor.
 * Please note all writes made within the aggregate context are durable.
 * 
 * @author meyersi
 */
public interface IStreamAggregator {
    /**
     * Aggregate a set of records received from the Kinesis Client Library.
     * 
     * @param records The set of Records received from a processRecords
     *        invocation
     * @throws Exception
     */
    public void aggregate(List<Record> records) throws Exception;

    public void aggregateEvents(List<InputEvent> events) throws Exception;

    /**
     * Commit all aggregated data to the backing store.
     */
    public void checkpoint() throws Exception;

    /**
     * Initialise the Aggregator on a shard. Should be called by
     * IRecordProcessor.initialize().
     */
    public void initialize(String shardId) throws Exception;

    /**
     * Terminate an Aggregator running, which will mark the process as offline
     * in the {@link InventoryModel} table.
     */
    public void shutdown(boolean flushState) throws Exception;

    /** Get the underlying data store name for the aggregator. */
    public String getTableName();
}

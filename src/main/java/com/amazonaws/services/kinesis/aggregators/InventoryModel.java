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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.kinesis.aggregators.datastore.DynamoUtils;

/**
 * Class used to provide configuration and setup for the Worker Inventory table
 * in Dynamo DB.
 */
@SuppressWarnings("serial")
public final class InventoryModel {
    private boolean online = false;

    /**
     * Name of the Table in Dynamo DB.
     */
    public static final String TABLE_NAME = "KinesisAggregatorWorkerState";

    /**
     * Column name used to store the Kinesis Stream Name for an Aggregator.
     */
    public static final String AGGREGATOR = "aggregator";

    /**
     * Column name used to store the Shard ID for an Aggregator.
     */
    public static final String SHARD_ID = "shardId";

    /**
     * Column name used to store the last time an Aggregator updated.
     */
    public static final String LAST_WRITE_TIME = "lastWriteTime";

    /**
     * Column name used to store the lowest sequence value updated in the last
     * flush of an Aggregator.
     */
    public static final String LAST_LOW_SEQ = "lastLowSeq";

    /**
     * Column name used to store the highest sequence value updated in the last
     * flush of an Aggregator.
     */
    public static final String LAST_HIGH_SEQ = "lastHighSeq";

    /**
     * Column name used to store the status of the running or stopped
     * Aggregator.
     */
    public static final String STATUS = "status";

    /**
     * Amount of read IOPS to provision for the Inventory table.
     */
    public static final long READ_CAPACITY = 10L;

    /**
     * Amount of write IOPS to provision for the Inventory table.
     */
    public static final long WRITE_CAPACITY = 10L;

    /**
     * Available states for an Aggregator to be in.
     */
    public static enum STATE {
        STARTING, RUNNING, STOPPED, SERIALISATION_ERROR, UNKNOWN_ERROR;
    }

    private AmazonDynamoDB dynamoClient;

    public InventoryModel(AmazonDynamoDB dynamoClient) throws Exception {
        this.dynamoClient = dynamoClient;
        init();
    }

    public InventoryModel(AWSCredentialsProvider credentials) throws Exception {
        this(new AmazonDynamoDBClient(credentials));
    }

    protected void init() throws Exception {
        List<AttributeDefinition> attributes = new ArrayList<AttributeDefinition>() {
            {
                add(new AttributeDefinition().withAttributeName(InventoryModel.AGGREGATOR).withAttributeType(
                        "S"));
                add(new AttributeDefinition().withAttributeName(InventoryModel.SHARD_ID).withAttributeType(
                        "S"));
            }
        };

        List<KeySchemaElement> key = new ArrayList<KeySchemaElement>() {
            {
                add(new KeySchemaElement().withAttributeName(InventoryModel.AGGREGATOR).withKeyType(
                        KeyType.HASH));
                add(new KeySchemaElement().withAttributeName(InventoryModel.SHARD_ID).withKeyType(
                        KeyType.RANGE));
            }
        };

        DynamoUtils.initTable(dynamoClient, InventoryModel.TABLE_NAME,
                InventoryModel.READ_CAPACITY, InventoryModel.WRITE_CAPACITY, attributes, key, null);

        online = true;
    }

    private Map<String, AttributeValue> getKey(final String streamName,
            final String applicationName, final String namespace, final String shardId) {
        return new HashMap<String, AttributeValue>() {
            {
                put(InventoryModel.AGGREGATOR, new AttributeValue().withS(String.format("%s.%s.%s",
                        streamName, applicationName, namespace)));
                put(InventoryModel.SHARD_ID, new AttributeValue().withS(shardId));
            }
        };
    }

    public void removeState(final String streamName, final String applicationName,
            final String namespace, final String shardId) throws Exception {
        DeleteItemRequest req = new DeleteItemRequest().withTableName(TABLE_NAME).withKey(
                getKey(streamName, applicationName, namespace, shardId));
        dynamoClient.deleteItem(req);
    }

    /**
     * Update the Inventory table with the state of an Aggregator.
     * 
     * @param streamName The Kinesis Stream being aggregated.
     * @param applicationName The application name running the aggregator.
     * @param workerId The worker ID which encapsulates an instance of an
     *        Aggregator.
     * @param lastLowSeq The lowest sequence number observed in all records
     *        which were flushed prior to this update.
     * @param lastHighSeq The highest sequence number for all records flushed in
     *        this update.
     * @param lastWriteTime The write time of the data to Dynamo DB.
     * @param status The {@link STATE} of the Aggregator.
     * @throws Exception
     */
    public void update(final String streamName, final String applicationName,
            final String namespace, final String shardId, final String lastLowSeq,
            final String lastHighSeq, final long lastWriteTime, final STATE status)
            throws Exception {
        // create the last write time value
        final String lastUpdateDateLabel = StreamAggregator.dateFormatter.format(new Date(
                lastWriteTime));
        // generate the item update
        Map<String, AttributeValueUpdate> inventoryUpdate = new HashMap<String, AttributeValueUpdate>() {
            {
                put(InventoryModel.LAST_WRITE_TIME,
                        new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
                                new AttributeValue().withS(lastUpdateDateLabel)));
                if (lastLowSeq != null)
                    put(InventoryModel.LAST_LOW_SEQ,
                            new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
                                    new AttributeValue().withS(lastLowSeq)));
                if (lastHighSeq != null)
                    put(InventoryModel.LAST_HIGH_SEQ,
                            new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
                                    new AttributeValue().withS(lastHighSeq)));
                if (status != null)
                    put(InventoryModel.STATUS,
                            new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
                                    new AttributeValue().withS(status.name())));
            }
        };
        DynamoUtils.updateWithRetries(
                dynamoClient,
                new UpdateItemRequest().withTableName(InventoryModel.TABLE_NAME).withKey(
                        getKey(streamName, applicationName, namespace, shardId)).withAttributeUpdates(
                        inventoryUpdate));
    }

    /**
     * Method which returns the update information for an Aggregator process.
     * 
     * @param streamName The Stream name which is being aggregated.
     * @param applicationName The application which is hosting the aggregator.
     * @param workerId The worker ID which is running an aggregator instance.
     * @return Tuple of Last Write Time (String), Last Low Sequence, and Last
     *         High Sequence
     */
    public InventoryStatus getLastUpdate(final String streamName, final String applicationName,
            final String namespace, final String shardId) {
        GetItemResult response = dynamoClient.getItem(InventoryModel.TABLE_NAME,
                getKey(streamName, applicationName, namespace, shardId));
        if (response.getItem() != null) {
            Map<String, AttributeValue> item = response.getItem();
            AttributeValue lastTime, lowSeq, highSeq = null;
            lastTime = item.get(InventoryModel.LAST_WRITE_TIME);
            lowSeq = item.get(InventoryModel.LAST_LOW_SEQ);
            highSeq = item.get(InventoryModel.LAST_HIGH_SEQ);

            return new InventoryStatus(lastTime == null ? null : lastTime.getS(),
                    lowSeq == null ? null : lowSeq.getS(), highSeq == null ? null : highSeq.getS());
        } else {
            return null;
        }
    }
}

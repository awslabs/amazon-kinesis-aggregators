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
package com.amazonaws.services.kinesis.aggregators.datastore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.Select;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.aggregators.AggregatorType;
import com.amazonaws.services.kinesis.aggregators.StreamAggregator;
import com.amazonaws.services.kinesis.aggregators.StreamAggregatorUtils;
import com.amazonaws.services.kinesis.aggregators.cache.UpdateKey;
import com.amazonaws.services.kinesis.aggregators.cache.UpdateValue;
import com.amazonaws.services.kinesis.aggregators.datastore.expressions.*;
import com.amazonaws.services.kinesis.aggregators.summary.SummaryCalculation;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;


public class DynamoDataStore implements IDataStore {
    public enum DynamoSummaryUpdateMethod {
        PUT(AttributeAction.PUT), ADD(AttributeAction.ADD), CONDITIONAL(null);
        private AttributeAction action;

        private DynamoSummaryUpdateMethod(AttributeAction a) {
            this.action = a;
        }

        public AttributeAction getAction() {
            return this.action;
        }
    }

    /**
     * The default amount of read IOPS to be provisioned, if the aggregator does
     * not override.
     */
    public static final long DEFAULT_READ_CAPACITY = 10L;

    /**
     * The default amount of write IOPS to be provisioned, if the aggregator
     * does not override.
     */
    public static final long DEFAULT_WRITE_CAPACITY = 10L;

    private final Log LOG = LogFactory.getLog(DynamoDataStore.class);

    private String environment, tableName, streamName;

    private AggregatorType aggregatorType;

    private boolean reportedStructure = false;

    private AmazonDynamoDB dynamoClient;

    private AmazonKinesisClient kinesisClient;

    private long readCapacity = DEFAULT_READ_CAPACITY;

    private long writeCapacity = DEFAULT_WRITE_CAPACITY;

    private String labelAttribute, dateAttribute;

    private boolean online = false;

    private Region region = Region.getRegion(Regions.US_EAST_1);

    public static final String SCATTER_PREFIX_ATTRIBUTE = "scatterPrefix";

    public static final int SCATTER_WIDTH = 99;

    private final Random r = new Random();

    private DynamoQueryEngine queryEngine;

    public DynamoDataStore(AmazonDynamoDB dynamoClient, AmazonKinesisClient kinesisClient,
            AggregatorType aggregatorType, String streamName, String tableName,
            String labelAttribute, String dateAttribute) {
        this.dynamoClient = dynamoClient;
        this.kinesisClient = kinesisClient;
        this.aggregatorType = aggregatorType;
        this.streamName = streamName;
        this.tableName = tableName;
        this.labelAttribute = labelAttribute;
        this.dateAttribute = dateAttribute;
    }

    public DynamoDataStore(AWSCredentialsProvider credentials, AggregatorType aggregatorType,
            String streamName, String tableName, String labelAttribute, String dateAttribute) {
        this(new AmazonDynamoDBAsyncClient(credentials), new AmazonKinesisClient(credentials),
                aggregatorType, streamName, tableName, labelAttribute, dateAttribute);
    }

    @Override
    public void initialise() throws Exception {
        if (!this.online) {
            if (this.region != null) {
                this.dynamoClient.setRegion(this.region);
                if (this.streamName != null) {
                    this.kinesisClient.setRegion(this.region);
                }
            }

            initAggTable(this.labelAttribute, this.dateAttribute, this.readCapacity,
                    this.writeCapacity);

            this.queryEngine = new DynamoQueryEngine(this.dynamoClient, this.tableName,
                    this.labelAttribute, this.dateAttribute);
            this.online = true;
        }
    }

    @Override
    public Map<UpdateKey, Map<String, AggregateAttributeModification>> write(
            Map<UpdateKey, UpdateValue> data) throws Exception {
        UpdateItemRequest req = null;
        UpdateItemResult result;
        Map<String, AggregateAttributeModification> updatedValues;
        Map<UpdateKey, Map<String, AggregateAttributeModification>> updatedData = new HashMap<>();

        int conditionals = 0;

        if (data != null && data.keySet().size() > 0) {
            LOG.debug(String.format("Flushing %s Cache Updates", data.size()));

            // go through all pending updates and write down increments to event
            // counts and SUM operations first, then do other types of
            // calculations which need conditional updates after
            for (final UpdateKey key1 : data.keySet()) {
                // initialise the map of all updates made for final value
                // processing
                if (!updatedData.containsKey(key1)) {
                    updatedValues = new HashMap<>();
                } else {
                    updatedValues = updatedData.get(key1);
                }

                Map<String, AttributeValueUpdate> updates = new HashMap<>();

                Map<String, String> attributeNames = new HashMap<String, String>();
                Map<String, AttributeValue> attributeValues = new HashMap<String, AttributeValue>();

                UpdateExpression.Clause<SetAction> sets = new UpdateExpression.Clause<SetAction>();
                UpdateExpression.Clause<AddAction> adds = new UpdateExpression.Clause<AddAction>();

                AttributeValueUpdate scatterUpdate = new AttributeValueUpdate()
                    .withAction(AttributeAction.PUT);

                attributeNames.put("#SCATTER_WIDTH", SCATTER_PREFIX_ATTRIBUTE);
                attributeValues.put(":scatter_width", new AttributeValue().withN("" + r.nextInt(SCATTER_WIDTH)));

                sets.add(new SetAction("#SCATTER_WIDTH", ":scatter_width"));

                // add the event count update to the list of updates to be made
                attributeNames.put("#EVENT_COUNT", StreamAggregator.EVENT_COUNT);
                attributeValues.put(":event_count", new AttributeValue().withN("" + data.get(key1).getAggregateCount()));

                adds.add(new AddAction("#EVENT_COUNT", ":event_count"));

                // add the time horizon type to the item
                attributeNames.put("#TIME_HORIZON_ATTR", StreamAggregator.TIME_HORIZON_ATTR);
                attributeValues.put(":time_horizon_attr", new AttributeValue().withS(key1.getTimeHorizon().getAbbrev()));

                sets.add(new SetAction("#TIME_HORIZON_ATTR", ":time_horizon_attr"));

                // add last update time and sequence
                attributeNames.put("#LAST_WRITE_SEQ", StreamAggregator.LAST_WRITE_SEQ);
                attributeValues.put(":last_write_seq", new AttributeValue().withS(data.get(key1).getLastWriteSeq()));

                sets.add(new SetAction("#LAST_WRITE_SEQ", ":last_write_seq"));

                // add last update time and sequence

                // TODO separately
                updates.put(
                        StreamAggregator.SAMPLES,
                        new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
                                new AttributeValue().withS(data.get(key1).getSamples().toString())));
                // end TODO

                attributeNames.put("#LAST_WRITE_TIME", StreamAggregator.LAST_WRITE_TIME);
                attributeValues.put(":last_write_time",
                    new AttributeValue()
                        .withS(
                            StreamAggregator.dateFormatter.format(
                                new Date(data.get(key1).getLastWriteTime())
                            )
                        )
                );

                sets.add(new SetAction("#LAST_WRITE_TIME", ":last_write_time"));

                if (this.aggregatorType.equals(AggregatorType.SUM)) {
                    for (final String attribute : data.get(key1).getSummaryValues().keySet()) {
                        final AggregateAttributeModification update = data.get(key1).getSummaryValues().get(
                                attribute);

                        if (!update.getCalculationApplied().getSummaryUpdateMethod().equals(
                                DynamoSummaryUpdateMethod.CONDITIONAL)) {

                            String setAttributeName = StreamAggregatorUtils.methodToColumn(attribute);

                            attributeNames.put("#" + setAttributeName.replace("-", ""), setAttributeName);
                            attributeValues.put(":" + setAttributeName.replace("-", ""),
                                new AttributeValue().withN("" + update.getFinalValue()));
                            adds.add(new AddAction("#" + setAttributeName.replace("-", ""), ":" + setAttributeName.replace("-", "")));

                            // add a stub entry so that we can extract the
                            // updated value from the resultset
                            updatedValues.put(setAttributeName, new AggregateAttributeModification(
                                    update.getAttributeName(), update.getOriginatingValueName(),
                                    null, update.getCalculationApplied()));
                        }
                    }
                }
                // do the update to all sum and count attributes as well
                // as the last write sequence and time - this gives us a key to
                // write other calculations onto

                ArrayList<Double> samples = data.get(key1).getSamples();

                // convert samples to attributeValues
                AttributeValue[] samplesAsAttributes = new AttributeValue[samples.size()];
                for (int i = 0; i < samples.size() ; i++) {
                    samplesAsAttributes[i] = new AttributeValue().withN("" + samples.get(i));
                }

                attributeNames.put("#samples", "samples");
                attributeValues.put(":samples", new AttributeValue().withL(samplesAsAttributes));
                attributeValues.put(":empty_list", new AttributeValue().withL(new AttributeValue[0]));
                String exists = new SetAction.IfNotExists("#samples", ":empty_list").toString();

                sets.add(new SetAction("#samples", new SetAction.Raw(new SetAction.Append(exists,":samples"))));

                req = new UpdateItemRequest().withTableName(tableName).withKey(
                        StreamAggregatorUtils.getTableKey(key1))
                        .withUpdateExpression(new UpdateExpression(sets, adds).toString())
                        .withExpressionAttributeNames(attributeNames)
                        .withExpressionAttributeValues(attributeValues)
                        .withReturnValues(ReturnValue.UPDATED_NEW);
                result = DynamoUtils.updateWithRetries(dynamoClient, req);

                // add the event count to the modifications made
                updatedValues.put(
                        StreamAggregator.EVENT_COUNT,
                        new AggregateAttributeModification(StreamAggregator.EVENT_COUNT,
                                StreamAggregator.EVENT_COUNT,
                                Double.parseDouble(result.getAttributes().get(
                                        StreamAggregator.EVENT_COUNT).getN()),
                                SummaryCalculation.SUM));

                // extract all updated values processed by the previous update
                for (String attribute : updatedValues.keySet()) {
                    updatedValues.put(
                            attribute,
                            new AggregateAttributeModification(
                                    updatedValues.get(attribute).getAttributeName(),
                                    updatedValues.get(attribute).getOriginatingValueName(),
                                    Double.parseDouble(result.getAttributes().get(attribute).getN()),
                                    updatedValues.get(attribute).getCalculationApplied(),
                                    updatedValues.get(attribute).getWritesSoFar() + 1));
                }

                // add all the updates for this key
                updatedData.put(key1, updatedValues);

                // log the structure of the table once, so the customer can
                // retrieve it directly
                if (!reportedStructure) {
                    LOG.info(getTableStructure());
                    reportedStructure = true;
                }
            }

            // now process all non summing calculations which are conditional
            // and
            // require that the table keys already exist
            if (this.aggregatorType.equals(AggregatorType.SUM)) {
                for (final UpdateKey key2 : data.keySet()) {
                    updatedValues = updatedData.get(key2);

                    // we perform a single update for all SUM operations and the
                    // count, last write sequence and time, and a
                    // separate conditional update for every instance of MIN or
                    // MAX
                    // calculations as these must be conditionally applied to be
                    // correct
                    for (final String attribute : data.get(key2).getSummaryValues().keySet()) {
                        final AggregateAttributeModification update = data.get(key2).getSummaryValues().get(
                                attribute);

                        if (update.getCalculationApplied().getSummaryUpdateMethod().equals(
                                DynamoSummaryUpdateMethod.CONDITIONAL)) {
                            conditionals++;
                            result = updateConditionalValue(dynamoClient, tableName, key2,
                                    attribute, update);

                            // if the update was made by this conditional
                            // update, then add its items to the update set
                            Double finalValue = null;
                            int increment = update.getWritesSoFar();
                            if (result != null && result.getAttributes() != null) {
                                finalValue = Double.parseDouble(result.getAttributes().get(
                                        attribute).getN());
                                increment++;
                            }
                            updatedValues.put(
                                    attribute,
                                    new AggregateAttributeModification(update.getAttributeName(),
                                            update.getOriginatingValueName(), finalValue,
                                            update.getCalculationApplied(), increment));

                        }
                    }

                    // add the conditional update items into the overall update
                    // set
                    updatedData.put(key2, updatedValues);
                }

                LOG.debug(String.format("Processed %s Conditional Updates", conditionals));
            }
        }

        return updatedData;
    }

    public UpdateItemResult updateConditionalValue(final AmazonDynamoDB dynamoClient,
            final String tableName, final UpdateKey key, final String attribute,
            final AggregateAttributeModification update) throws Exception {
        Map<String, AttributeValue> updateKey = StreamAggregatorUtils.getTableKey(key);
        UpdateItemResult result;
        final ReturnValue returnValue = ReturnValue.UPDATED_NEW;
        final String setAttribute = StreamAggregatorUtils.methodToColumn(attribute);

        // create the update that we want to write
        final Map<String, AttributeValueUpdate> thisCalcUpdate = new HashMap<String, AttributeValueUpdate>() {
            {
                put(setAttribute,
                        new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(
                                new AttributeValue().withN("" + update.getFinalValue())));
            }
        };
        // create the request
        UpdateItemRequest req = new UpdateItemRequest().withTableName(tableName).withKey(updateKey).withReturnValues(
                returnValue).withAttributeUpdates(thisCalcUpdate);

        Map<String, ExpectedAttributeValue> expected = new HashMap<>();

        final SummaryCalculation calc = update.getCalculationApplied();

        // try an update to PUT the value if NOT EXISTS, to establish if we
        // are the first writer for this key
        expected = new HashMap<String, ExpectedAttributeValue>() {
            {
                put(setAttribute, new ExpectedAttributeValue().withExists(false));
            }
        };

        req.setExpected(expected);

        try {
            result = DynamoUtils.updateWithRetries(dynamoClient, req);

            // yay - we were the first writer, so our value was written
            return result;
        } catch (ConditionalCheckFailedException e1) {
            // set the expected to the comparison contained in the update
            // calculation
            expected.clear();
            expected.put(
                    setAttribute,
                    new ExpectedAttributeValue().withComparisonOperator(
                            calc.getDynamoComparisonOperator()).withValue(
                            new AttributeValue().withN("" + update.getFinalValue())));
            req.setExpected(expected);

            // do the conditional update on the summary
            // calculation. this may result in no update being
            // applied because the new value is greater than the
            // current minimum for MIN, or less than the current
            // maximum for MAX.
            try {
                result = DynamoUtils.updateWithRetries(dynamoClient, req);

                return result;
            } catch (ConditionalCheckFailedException e2) {
                // no worries - we just weren't the min or max!
                return null;
            }
        }
    }

    /**
     * Method which examines an table which backs an Aggregator, and returns a
     * string value which represents the list of attributes in the table. This
     * method assumes that all elements in an aggregate table are the same.
     *
     * @param dynamoClient Dynamo DB Client to use for connection to Dynamo DB.
     * @param dynamoTable The Table to get the structure of.
     * @return A String representation of the attribute names in the table.
     * @throws Exception
     */
    public String getTableStructure() throws Exception {
        List<String> columns = getDictionaryEntry();
        StringBuffer sb = new StringBuffer();
        for (String s : columns) {
            sb.append(String.format("%s,", s));
        }
        return String.format("Dynamo Table %s (%s)", sb.toString().substring(0, sb.length() - 1),
                this.tableName);
    }

    /**
     * Generate a list of attribute names found in the Aggregator's dynamo
     * table. Assumes that all Items in the Aggregator table are of the same
     * structure.
     *
     * @param dynamoClient Dynamo DB Client to use for connection to Dynamo DB.
     * @param dynamoTable The Dynamo Table for the Aggregator
     * @return A list of attribute names from the Dynamo table
     * @throws Exception
     */
    protected List<String> getDictionaryEntry() throws Exception {
        // get a list of all columns in the table, with keys first
        List<String> columns = new ArrayList<>();
        List<KeySchemaElement> keys = dynamoClient.describeTable(this.tableName).getTable().getKeySchema();
        for (KeySchemaElement key : keys) {
            columns.add(key.getAttributeName());
        }
        ScanResult scan = dynamoClient.scan(new ScanRequest().withTableName(this.tableName).withSelect(
                Select.ALL_ATTRIBUTES).withLimit(1));
        List<Map<String, AttributeValue>> scannedItems = scan.getItems();
        for (Map<String, AttributeValue> map : scannedItems) {
            for (String s : map.keySet()) {
                if (!columns.contains(s))
                    columns.add(s);
            }
        }

        return columns;
    }

    /*
     * Configure the aggregate table with the indicated capacity, including
     * global secondary index on last_write_seq for facilitating aggregate
     * cleanup
     */
    public void initAggTable(final String keyColumn, final String dateColumnName,
            final long readCapacity, final long writeCapacity) throws Exception {
        final String setDateColumn = dateColumnName == null ? StreamAggregator.DEFAULT_DATE_VALUE
                : dateColumnName;

        long setReadCapacity = readCapacity == -1 ? DEFAULT_READ_CAPACITY : readCapacity;
        long setWriteCapacity = writeCapacity == -1 ? DEFAULT_WRITE_CAPACITY : writeCapacity;

        // we have to add this attribute list so that we can project the key
        // into the GSI
        List<AttributeDefinition> attributes = new ArrayList<AttributeDefinition>() {
            {
                add(new AttributeDefinition().withAttributeName(keyColumn).withAttributeType("S"));
                add(new AttributeDefinition().withAttributeName(setDateColumn).withAttributeType(
                        "S"));
            }
        };

        Collection<GlobalSecondaryIndex> gsi = new ArrayList<>();

        // Global Secondary Index for accessing the table by date item
        gsi.add(new GlobalSecondaryIndex().withIndexName(
                StreamAggregatorUtils.getDateDimensionIndexName(tableName, setDateColumn)).withKeySchema(
                new KeySchemaElement().withAttributeName(SCATTER_PREFIX_ATTRIBUTE).withKeyType(
                        KeyType.HASH),
                new KeySchemaElement().withAttributeName(setDateColumn).withKeyType(KeyType.RANGE)).withProjection(
                new Projection().withProjectionType(ProjectionType.KEYS_ONLY)).withProvisionedThroughput(
                new ProvisionedThroughput().withReadCapacityUnits(setReadCapacity).withWriteCapacityUnits(
                        setWriteCapacity)));

        attributes.add(new AttributeDefinition().withAttributeName(SCATTER_PREFIX_ATTRIBUTE).withAttributeType(
                "N"));

        // table is hash/range on value and date
        List<KeySchemaElement> key = new ArrayList<KeySchemaElement>() {
            {
                add(new KeySchemaElement().withAttributeName(keyColumn).withKeyType(KeyType.HASH));
                add(new KeySchemaElement().withAttributeName(setDateColumn).withKeyType(
                        KeyType.RANGE));
            }
        };

        // initialise the table
        DynamoUtils.initTable(this.dynamoClient, this.tableName, setReadCapacity, setWriteCapacity,
                attributes, key, gsi);
    }

    public long refreshForceCheckpointThresholds() {
        LOG.info("Refreshing Provisioned Throughput settings");

        // get the current provisioned capacity
        this.writeCapacity = getProvisionedWrites();

        // get the current number of provisioned kinesis shards for the stream,
        // if we know what stream we are working against
        int currentShardCount = 1;
        if (this.streamName != null) {
            try {
                currentShardCount = StreamAggregatorUtils.getShardCount(this.kinesisClient,
                        this.streamName);
                return (4 * (60 * this.writeCapacity)) / currentShardCount;
            } catch (Exception e) {
                LOG.warn(String.format(
                        "Unable to get Shard Count for Stream %s. Using Overly Optimistic Throughput Settings",
                        this.streamName));
            }
        }
        return (4 * (60 * this.writeCapacity));
    }

    private long getProvisionedWrites() {
        return dynamoClient.describeTable(this.tableName).getTable().getProvisionedThroughput().getWriteCapacityUnits();
    }

    public DynamoQueryEngine queryEngine() {
        return this.queryEngine;
    }

    public Region getRegion() {
        return this.region;
    }

    @Override
    public void setRegion(Region region) {
        this.region = region;
    }

    public DynamoDataStore withStorageCapacity(long readCapacity, long writeCapacity) {
        if (readCapacity > 0l)
            this.readCapacity = readCapacity;

        if (writeCapacity > 0l)
            this.writeCapacity = writeCapacity;

        return this;
    }
}

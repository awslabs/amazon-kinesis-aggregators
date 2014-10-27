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

import java.io.File;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.aggregators.cache.UpdateKey;
import com.amazonaws.services.kinesis.aggregators.datastore.DynamoUtils;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Utility methods used across the Amazon Kinesis Aggregators framework.
 */
public class StreamAggregatorUtils {
    private static final Log LOG = LogFactory.getLog(StreamAggregatorUtils.class);

    private static final String rsTimeformat = "yyyy-mm-dd hh:mi:ss";

    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        mapper.configure(DeserializationFeature.WRAP_EXCEPTIONS, false);
    }

    private StreamAggregatorUtils() {
    }

    /**
     * Helper method which converts input values from Aggregator configurations
     * into names for attributes in Dynamo DB. In particular this supports
     * Object based Aggregators who will be configured using get methods. For
     * example, this methods will turn 'getValue' into 'value' and 'isSomething'
     * to 'isSomething'.
     * 
     * @param methodName The name of the method to be converted into an
     *        Attribute Name.
     * @return A string value to be used as the corresponding attribute name.
     */
    public static String methodToColumn(String methodName) {
        if (methodName.startsWith("get")) {
            return methodName.substring(3, 4).toLowerCase() + methodName.substring(4);
        } else {
            return methodName.substring(0, 1).toLowerCase() + methodName.substring(1);
        }
    }

    /**
     * Returns a statement which can be used to create an External Table in Hive
     * which wraps the Aggregator Table indicated, using the required name in
     * Hive.
     * 
     * @param dynamoClient Dynamo DB Client to use for connection to Dynamo DB.
     * @param hiveTableName The table name to generate for the Hive Table.
     * @param dynamoTable The name of the aggregator table in Dynamo DB.
     * @return A CREATE EXTERNAL TABLE statement to be used in Hive
     * @throws Exception
     */
    public static String getDynamoHiveWrapper(AmazonDynamoDB dynamoClient, String hiveTableName,
            String dynamoTable) throws Exception {
        LOG.info("Generating Hive Integration Statement");

        StringBuffer sb = new StringBuffer();
        sb.append(String.format("CREATE EXTERNAL TABLE %s(", hiveTableName));

        // add the hive table spec
        List<String> tableDefinition = DynamoUtils.getDictionaryEntry(dynamoClient, dynamoTable);
        for (String s : tableDefinition) {
            sb.append(String.format("%s string,", s));
        }
        sb.replace(sb.length() - 1, sb.length(), "");

        sb.append(String.format(
                ") STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler' TBLPROPERTIES (\"dynamodb.table.name\" = \"%s\", \"dynamodb.column.mapping\" = \"",
                dynamoTable));
        for (String s : tableDefinition) {
            sb.append(String.format("%s:%s,", s, s));
        }
        sb.replace(sb.length() - 1, sb.length(), "");
        sb.append("))");

        return sb.toString();
    }

    /**
     * Helper method to generate a Redshift CREATE TABLE command which matches
     * the structure of the aggregate table, and a COPY command which will load
     * the table data from an Aggregator table into a Redshift Table. * @param
     * dynamoClient Dynamo DB Client to use for connection to Dynamo DB.
     * 
     * @param redshiftTableName The table name to use in Redshift.
     * @param dynamoTable The Aggregator table name in Dynamo DB.
     * @return A String which contains the create table and copy commands to be
     *         issued against redshift.
     */
    public static String getRedshiftCopyCommand(final AmazonDynamoDB dynamoClient,
            String redshiftTableName, String dynamoTable) throws Exception {
        LOG.info("Generating Redshift Copy Command");
        StringBuffer sb = new StringBuffer();

        // generate the create table statement
        sb.append(String.format("CREATE TABLE %S(\n", redshiftTableName));
        int i = 0;
        List<String> tableStructure = DynamoUtils.getDictionaryEntry(dynamoClient, dynamoTable);
        String columnSpec = null;
        String dataType = null;
        for (String s : tableStructure) {
            i++;

            switch (s) {
                case StreamAggregator.LAST_WRITE_SEQ:
                    dataType = "BIGINT";
                    break;
                case StreamAggregator.LAST_WRITE_TIME:
                    dataType = "TIMESTAMP";
                    break;
                case StreamAggregator.EVENT_COUNT:
                    dataType = "INT";
                    break;
                default:
                    if (s.contains("-SUM") || s.contains("-MIN") || s.contains("-MAX")) {
                        dataType = "INT";
                    } else {
                        dataType = "VARCHAR(1000)";
                    }
                    break;
            }
            ;

            columnSpec = s + " " + dataType;

            if (i == tableStructure.size()) {
                sb.append(columnSpec);
            } else {
                sb.append(columnSpec + ",");
            }
        }
        sb.append(");\n\n");

        // generate the copy command
        sb.append(String.format(
                "copy %s from 'dynamodb://%s' credentials 'aws_access_key_id=<Your-Access-Key-ID>;aws_secret_access_key=<Your-Secret-Access-Key>' readratio 50 timeformat 'yyyy-MM-dd hh:mi:ss';",
                redshiftTableName, dynamoTable, rsTimeformat));
        return sb.toString();
    }

    /**
     * Index name which should be used for the last write sequence GSI on a
     * table
     * 
     * @param dynamoTable The table name in Dynamo DB.
     * @return The name for the global secondary index on the table for last
     *         write sequence.
     */
    public static final String getLastWriteSeqIndexName(String dynamoTable) {
        return dynamoTable + "-seq";
    }

    /**
     * Index name which should be used for the last write sequence GSI on a
     * table
     * 
     * @param dynamoTable The table name in Dynamo DB.
     * @return The name for the global secondary index on the table for last
     *         write sequence.
     */
    public static final String getDateDimensionIndexName(String dynamoTable, String dateAttribute) {
        return String.format("%s-%s", dynamoTable, dateAttribute);
    }

    /**
     * Method which will generate a correctly formatted primary key for a dynamo
     * table hosting aggregated data.
     * 
     * @param updateKey An {@link UpdateKey} which should be pivoted into a key.
     * @return
     */
    public static Map<String, AttributeValue> getTableKey(UpdateKey updateKey) {
        return getTableKey(updateKey.getAggregateColumnName(), updateKey.getAggregatedValue(),
                updateKey.getDateValueColumnName(), updateKey.getDateValue());
    }

    /**
     * Method which will generate a correctly formatted primary key for a dynamo
     * table hosting aggregated data.
     * 
     * @param keyColumnName The attribute name in the table to be used as the
     *        first part of a hash key.
     * @param fieldValue The value of the hash key to query for.
     * @param dateColumnName The attribute name of the date column to be used as
     *        the range key.
     * @param dateValue The value of the range key value to query for.
     * @return
     */
    protected static Map<String, AttributeValue> getTableKey(String keyColumnName,
            String fieldValue, String dateColumnName, String dateValue) {
        HashMap<String, AttributeValue> key = new HashMap<>();
        key.put(keyColumnName, new AttributeValue().withS(fieldValue));
        key.put(dateColumnName, new AttributeValue().withS(dateValue));

        return key;
    }

    protected static Map<String, AttributeValue> getValue(final AmazonDynamoDB dynamoClient,
            final String tableName, final UpdateKey key) {
        GetItemRequest req = new GetItemRequest().withTableName(tableName).withKey(getTableKey(key));
        return dynamoClient.getItem(req).getItem();
    }

    protected static String getTableName(final String applicationName, final String namespace) {
        return String.format("%s-%s", applicationName, namespace);
    }

    public static JsonNode asJsonNode(String s) throws Exception {
        return mapper.readTree(s);
    }

    public static JsonNode asJsonNode(File f) throws Exception {
        return mapper.readTree(f);
    }

    public static JsonNode readJsonValue(JsonNode json, String atPath) {
        if (!atPath.contains(".")) {
            return json.get(atPath);
        } else {
            String[] path = atPath.split("\\.");

            JsonNode node = json.get(path[0]);
            for (int i = 1; i < path.length; i++) {
                node = node.path(path[i]);
            }

            return node;
        }
    }

    public static String readValueAsString(JsonNode json, String atPath) {
        JsonNode node = readJsonValue(json, atPath);

        return node == null ? null : node.asText();
    }

    /**
     * Get a list of all Open shards ordered by their start hash
     * 
     * @param streamName
     * @return A Map of only Open Shards indexed by the Shard ID
     */
    public static Map<String, Shard> getOpenShards(AmazonKinesisClient kinesisClient,
            String streamName) throws Exception {
        Map<String, Shard> shardMap = new LinkedHashMap<>();
        final int BACKOFF_MILLIS = 10;
        final int MAX_DESCRIBE_ATTEMPTS = 10;
        int describeAttempts = 0;
        StreamDescription stream = null;
        try {
            do {
                try {
                    stream = kinesisClient.describeStream(streamName).getStreamDescription();
                } catch (LimitExceededException e) {
                    Thread.sleep(2 ^ describeAttempts * BACKOFF_MILLIS);
                    describeAttempts++;
                }
            } while (stream == null && describeAttempts < MAX_DESCRIBE_ATTEMPTS);
        } catch (InterruptedException e) {
            LOG.error(e);
            throw e;
        }

        if (stream == null) {
            throw new Exception(String.format("Unable to describe Stream after %s attempts",
                    MAX_DESCRIBE_ATTEMPTS));
        }
        Collection<String> openShardNames = new ArrayList<String>();

        // load all the shards on the stream
        for (Shard shard : stream.getShards()) {
            openShardNames.add(shard.getShardId());
            shardMap.put(shard.getShardId(), shard);

            // remove this shard's parents from the set of active shards -
            // we
            // can't do anything to them
            if (shard.getParentShardId() != null) {
                openShardNames.remove(shard.getParentShardId());
            }
            if (shard.getAdjacentParentShardId() != null) {
                openShardNames.remove(shard.getAdjacentParentShardId());
            }
        }

        // create a List of Open shards for sorting
        List<Shard> shards = new ArrayList<Shard>();
        for (String s : openShardNames) {
            shards.add(shardMap.get(s));
        }

        // sort the list into lowest start hash order
        Collections.sort(shards, new Comparator<Shard>() {
            public int compare(Shard o1, Shard o2) {
                return new BigInteger(o1.getHashKeyRange().getStartingHashKey()).compareTo(new BigInteger(
                        o2.getHashKeyRange().getStartingHashKey()));
            }
        });

        // rebuild the shard map into the correct order
        shardMap.clear();
        for (Shard s : shards) {
            shardMap.put(s.getShardId(), s);
        }

        return shardMap;

    }

    public static Shard getFirstShard(AmazonKinesisClient kinesisClient, String streamName)
            throws Exception {
        return getOpenShards(kinesisClient, streamName).values().iterator().next();
    }

    public static String getFirstShardName(AmazonKinesisClient kinesisClient, String streamName)
            throws Exception {
        return getFirstShard(kinesisClient, streamName).getShardId();
    }

    public static int getShardCount(AmazonKinesisClient kinesisClient, String streamName)
            throws Exception {
        return getOpenShards(kinesisClient, streamName).keySet().size();
    }
}

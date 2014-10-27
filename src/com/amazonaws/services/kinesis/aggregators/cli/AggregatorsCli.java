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
package com.amazonaws.services.kinesis.aggregators.cli;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.aggregators.AggregatorType;
import com.amazonaws.services.kinesis.aggregators.AggregatorsConstants;
import com.amazonaws.services.kinesis.aggregators.StreamAggregatorUtils;
import com.amazonaws.services.kinesis.aggregators.datastore.DynamoUtils;

public class AggregatorsCli {
    public static final String DELETE_TO_HWM = "delete-to-hwm";

    public static final String GET_REDSHIFT_COPY = "get-redshift-copy-command";

    public static final String GET_HIVE_WRAPPER = "get-hive-wrapper-statement";

    public static final String GET_TABLE_STRUCTURE = "get-dynamo-table-structure";

    private static void validateAction(String actionRequested) throws Exception {
        if (!actionRequested.equals(DELETE_TO_HWM) && !actionRequested.equals(GET_REDSHIFT_COPY)
                && !actionRequested.equals(GET_HIVE_WRAPPER)
                && !actionRequested.equals(GET_TABLE_STRUCTURE))
            throw new Exception(String.format("Invalid Action %s", actionRequested));
    }

    public static void main(String[] args) throws Exception {
        String applicationName = System.getProperty(AggregatorsConstants.APP_NAME_PARAM);
        String namespace = System.getProperty(AggregatorsConstants.NAMESPACE_PARAM);
        String action = System.getProperty("action");
        String regionName = System.getProperty(AggregatorsConstants.REGION_PARAM);
        Region region = null;
        if (regionName != null && !regionName.equals("")) {
            region = Region.getRegion(Regions.fromName(regionName));
        }

        validateAction(action);

        final AWSCredentialsProvider credentialsProvider;

        final String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
        if (accessKey == null) {
            credentialsProvider = new ClasspathPropertiesFileCredentialsProvider();
        } else {
            credentialsProvider = new EnvironmentVariableCredentialsProvider();
        }

        String aggregatorTableName;
        final AmazonDynamoDB dynamoClient = new AmazonDynamoDBClient(credentialsProvider);
        if (region != null)
            dynamoClient.setRegion(region);

        switch (action) {
            case DELETE_TO_HWM:
                String hwm = System.getProperty("last-sequence-number");
                aggregatorTableName = System.getProperty("from-aggregator-table");

                DynamoUtils.cleanupAggTable(credentialsProvider, region, aggregatorTableName, hwm);
                break;
            case GET_REDSHIFT_COPY:
                // get the redshift target table name
                String redshiftTableName = System.getProperty("to-redshift-table");
                aggregatorTableName = System.getProperty("from-aggregator-table");

                System.out.println(StreamAggregatorUtils.getRedshiftCopyCommand(dynamoClient,
                        redshiftTableName, aggregatorTableName));
                break;
            case GET_HIVE_WRAPPER:
                AggregatorType aggType = AggregatorType.valueOf(System.getProperty("aggregator-type"));
                String hiveTableName = System.getProperty("hive-table-name");
                aggregatorTableName = System.getProperty("from-aggregator-table");

                System.out.println(StreamAggregatorUtils.getDynamoHiveWrapper(dynamoClient,
                        hiveTableName, aggregatorTableName));
                break;
            case GET_TABLE_STRUCTURE:
                aggregatorTableName = System.getProperty("from-aggregator-table");
                System.out.println(DynamoUtils.getDynamoTableStructure(dynamoClient,
                        aggregatorTableName));
                break;
            default:
                break;
        }
    }
}

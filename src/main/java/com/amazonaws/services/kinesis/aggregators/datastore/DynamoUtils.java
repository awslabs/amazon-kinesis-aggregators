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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LimitExceededException;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.Select;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.kinesis.aggregators.StreamAggregator;

public class DynamoUtils {
	private static final Log LOG = LogFactory.getLog(DynamoUtils.class);

	private DynamoUtils() {
	}

	/**
	 * Private interface for creating tables which handles any instances of
	 * Throttling of the API
	 * 
	 * @param dynamoClient
	 * @param dynamoTable
	 * @return
	 * @throws Exception
	 */
	public static CreateTableResult safeCreateTable(
			final AmazonDynamoDB dynamoClient,
			final CreateTableRequest createTableRequest) throws Exception {
		CreateTableResult res = null;
		final int tryMax = 10;
		int tries = 0;
		while (true) {
			try {
				res = dynamoClient.createTable(createTableRequest);
				return res;
			} catch (LimitExceededException le) {
				if (tries < tryMax) {
					// back off for 1 second
					Thread.sleep(1000);
					tries++;
				} else {
					throw le;
				}
			} catch (ResourceInUseException rie) {
				// someone else is trying to create the table while we are, so
				// return ok
				return null;
			}
		}
	}

	/**
	 * Creates a table in Dynamo DB with the requested read and write capacity,
	 * attributes, key schema and GSI's. This method will block until the table
	 * is Active in Dynamo DB.
	 * 
	 * @param dynamoClient
	 *            Dynamo DB Client to use for connection to Dynamo DB.
	 * @param dynamoTable
	 *            The table name to create in Dynamo DB.
	 * @param readCapacity
	 *            The requested amount of read IOPS to be provisioned.
	 * @param writeCapacity
	 *            The requested amount of write IOPS to be provisioned.
	 * @param attributes
	 *            Attribute Names which must be indicated to create the key
	 *            schema and/or GSI's.
	 * @param keySchema
	 *            The keys used for the primary key of the table.
	 * @param gsi
	 *            List of Global Secondary Indexes to be created on the table
	 * @throws Exception
	 */
	public static void initTable(final AmazonDynamoDB dynamoClient,
			final String dynamoTable, final long readCapacity,
			final long writeCapacity, List<AttributeDefinition> attributes,
			List<KeySchemaElement> keySchema,
			final Collection<GlobalSecondaryIndex> gsi) throws Exception {
		try {
			DescribeTableResult res = safeDescribeTable(dynamoClient,
					dynamoTable);

			if (!res.getTable().getTableStatus().equals("ACTIVE")) {
				waitForTableActive(dynamoClient, dynamoTable);
			}
		} catch (ResourceInUseException r) {
			waitForTableActive(dynamoClient, dynamoTable);
		} catch (ResourceNotFoundException e) {
			LOG.info(String
					.format("Table %s Not Found - Creating with %s Reads/sec & %s Writes/sec",
							dynamoTable, readCapacity, writeCapacity));

			CreateTableRequest createTableRequest = new CreateTableRequest()
					.withTableName(dynamoTable)
					.withProvisionedThroughput(
							new ProvisionedThroughput().withReadCapacityUnits(
									readCapacity).withWriteCapacityUnits(
									writeCapacity)).withKeySchema(keySchema)
					.withAttributeDefinitions(attributes);

			if (gsi != null)
				createTableRequest.withGlobalSecondaryIndexes(gsi);

			// create the table
			try {
				safeCreateTable(dynamoClient, createTableRequest);
			} catch (Exception ex) {
				LOG.error(ex);
				throw e;
			}

			// wait for it to go to active state
			waitForTableActive(dynamoClient, dynamoTable);
		}
	}

	/**
	 * Private interface for describing tables which handles any instances of
	 * Throttling of the API
	 * 
	 * @param dynamoClient
	 * @param dynamoTable
	 * @return
	 * @throws Exception
	 */
	public static DescribeTableResult safeDescribeTable(
			final AmazonDynamoDB dynamoClient, final String dynamoTable)
			throws Exception {
		DescribeTableResult res = null;
		final int tryMax = 10;
		int tries = 0;
		while (true) {
			try {
				res = dynamoClient.describeTable(dynamoTable);

				return res;
			} catch (ResourceNotFoundException e) {
				if (tries < tryMax) {
					// sleep for a short time as this is potentially an eventual
					// consistency issue with the table having been created ms
					// ago
					Thread.sleep(10);
					tries++;
				} else {
					throw e;
				}
			}
		}
	}

	/**
	 * Method which waits for a Dynamo table to enter status 'Active'.
	 * 
	 * @param dynamoClient
	 *            Dynamo DB Client to use for connection to Dynamo DB.
	 * @param dynamoTable
	 *            The Table in Dynamo.
	 * @throws Exception
	 */
	public static void waitForTableActive(final AmazonDynamoDB dynamoClient,
			final String dynamoTable) throws Exception {
		waitForTableState(dynamoClient, dynamoTable, TableStatus.ACTIVE);
	}

	/**
	 * Interface which will block until a dynamo table reaches a specified
	 * state. Also returns immediately if the object doesn't exist
	 * 
	 * @param dynamoClient
	 *            Dynamo DB Client to use for connection to Dynamo DB.
	 * @param dynamoTable
	 *            The table name to check.
	 * @param status
	 *            The status to wait for
	 * @throws Exception
	 */
	private static void waitForTableState(final AmazonDynamoDB dynamoClient,
			final String dynamoTable, TableStatus status) throws Exception {
		DescribeTableResult tableRequest = null;
		while (true) {
			try {
				tableRequest = dynamoClient.describeTable(dynamoTable);
				if (tableRequest.getTable().getTableStatus()
						.equals(status.name()))
					break;

				Thread.sleep(1000);
			} catch (InterruptedException e) {
				return;
			}
		}
	}

	public static void dropTable(final AmazonDynamoDB dynamoClient,
			final String dynamoTable) throws Exception {
		if (dynamoTable != null) {
			LOG.info(String.format("Dropping Dynamo Table %s", dynamoTable));
			try {
				dynamoClient.deleteTable(dynamoTable);
				waitForTableState(dynamoClient, dynamoTable,
						TableStatus.DELETING);
			} catch (ResourceNotFoundException e) {
				LOG.info("OK - Table Not Found");
			}
		}
	}

	public static void cleanupAggTable(AWSCredentialsProvider credentials,
			Region region, final String dynamoTable, final String toSeq)
			throws Exception {
		final Double deleteBelow = Double.parseDouble(toSeq);

		// create two clients - one synchronous for the read of all candidate
		// values, and another for the delete operations
		final AmazonDynamoDB dynamoClient = new AmazonDynamoDBClient(
				credentials);
		if (region != null)
			dynamoClient.setRegion(region);
		final AmazonDynamoDBAsyncClient deleteCli = new AmazonDynamoDBAsyncClient(
				credentials);
		deleteCli.setRegion(region);
		Map<String, AttributeValue> lastKey = null;
		Map<String, AttributeValue> deleteKey = null;

		// work out what the key and date column name is
		String keyColumn = null;
		String dateColumn = null;

		List<KeySchemaElement> keySchema = dynamoClient
				.describeTable(dynamoTable).getTable().getKeySchema();
		for (KeySchemaElement element : keySchema) {
			if (element.getKeyType().equals(KeyType.HASH.name()))
				keyColumn = element.getAttributeName();

			if (element.getKeyType().equals(KeyType.RANGE.name()))
				dateColumn = element.getAttributeName();
		}

		LOG.info(String.format(
				"Deleting data from %s where %s values are below %s",
				dynamoTable, StreamAggregator.LAST_WRITE_SEQ, deleteBelow));
		int deleteCount = 0;

		do {
			// read data from the table
			ScanRequest scan = new ScanRequest()
					.withTableName(dynamoTable)
					.withAttributesToGet(keyColumn, dateColumn,
							StreamAggregator.LAST_WRITE_SEQ)
					.withExclusiveStartKey(lastKey);

			ScanResult results = dynamoClient.scan(scan);

			// delete everything up to the system provided change number
			for (Map<String, AttributeValue> map : results.getItems()) {
				deleteKey = new HashMap<>();
				deleteKey.put(keyColumn, map.get(keyColumn));
				deleteKey.put(dateColumn, map.get(dateColumn));

				if (Double.parseDouble(map.get(StreamAggregator.LAST_WRITE_SEQ)
						.getS()) < deleteBelow) {
					deleteCli.deleteItem(dynamoTable, deleteKey);
					deleteCount++;
				}
			}
			lastKey = results.getLastEvaluatedKey();
		} while (lastKey != null);

		LOG.info(String.format(
				"Operation Complete - %s Records removed from Aggregate Store",
				deleteCount));
	}

	public static UpdateItemResult updateWithRetries(
			AmazonDynamoDB dynamoClient, UpdateItemRequest req)
			throws Exception {
		final double initialBackoff = 2D;
		final int updateRetries = 10;
		final double backoffRatio = 1.2;

		double backoff = initialBackoff;

		UpdateItemResult res = null;

		for (int i = 0; i < updateRetries; i++) {
			try {
				res = dynamoClient.updateItem(req);
				break;
			} catch (ProvisionedThroughputExceededException ptee) {
				LOG.warn(String.format(
						"Exceeded Provisioned Througput - Backing off for %s",
						backoff));
				try {
					Thread.sleep(new Double(backoff).longValue());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				// simple linear backoff
				backoff = backoff * backoffRatio;
			} catch (ConditionalCheckFailedException ccfe) {
				// silently rethrow these exceptions as they are part of the
				// conditional update logic for MIN/MAX calculations
				throw ccfe;
			} catch (Exception e) {
				LOG.warn(e);
				throw e;
			}
		}

		if (res == null) {
			throw new Exception(String.format(
					"Unable to write after %s retries", updateRetries));
		} else {
			return res;
		}
	}

	/**
	 * Method which examines an table which backs an Aggregator, and returns a
	 * string value which represents the list of attributes in the table. This
	 * method assumes that all elements in an aggregate table are the same.
	 * 
	 * @param dynamoClient
	 *            Dynamo DB Client to use for connection to Dynamo DB.
	 * @param dynamoTable
	 *            The Table to get the structure of.
	 * @return A String representation of the attribute names in the table.
	 * @throws Exception
	 */
	public static String getDynamoTableStructure(AmazonDynamoDB dynamoClient,
			String dynamoTable) throws Exception {
		List<String> columns = getDictionaryEntry(dynamoClient, dynamoTable);
		StringBuffer sb = new StringBuffer();
		for (String s : columns) {
			sb.append(String.format("%s,", s));
		}
		return String.format("Dynamo Table %s (%s)",
				sb.toString().substring(0, sb.length() - 1), dynamoTable);
	}

	/**
	 * Generate a list of attribute names found in the Aggregator's dynamo
	 * table. Assumes that all Items in the Aggregator table are of the same
	 * structure.
	 * 
	 * @param dynamoClient
	 *            Dynamo DB Client to use for connection to Dynamo DB.
	 * @param dynamoTable
	 *            The Dynamo Table for the Aggregator
	 * @return A list of attribute names from the Dynamo table
	 * @throws Exception
	 */
	public static List<String> getDictionaryEntry(
			final AmazonDynamoDB dynamoClient, final String dynamoTable)
			throws Exception {
		// get a list of all columns in the table, with keys first
		List<String> columns = new ArrayList<>();
		List<KeySchemaElement> keys = dynamoClient.describeTable(dynamoTable)
				.getTable().getKeySchema();
		for (KeySchemaElement key : keys) {
			columns.add(key.getAttributeName());
		}
		ScanResult scan = dynamoClient.scan(new ScanRequest()
				.withTableName(dynamoTable).withSelect(Select.ALL_ATTRIBUTES)
				.withLimit(1));
		List<Map<String, AttributeValue>> scannedItems = scan.getItems();
		for (Map<String, AttributeValue> map : scannedItems) {
			for (String s : map.keySet()) {
				if (!columns.contains(s))
					columns.add(s);
			}
		}

		return columns;
	}

	public static List<Map<String, AttributeValue>> queryUntilDone(
			AmazonDynamoDB dynamoClient, QueryRequest qr, int backoffMillis)
			throws Exception {
		List<Map<String, AttributeValue>> output = new ArrayList<>();

		Map<String, AttributeValue> lastKeyEvaluated = null;
		do {
			int queryAttempts = 0;
			QueryResult result = null;

			do {
				try {
					result = dynamoClient.query(qr).withLastEvaluatedKey(
							lastKeyEvaluated);

					output.addAll(result.getItems());
				} catch (ProvisionedThroughputExceededException e) {
					LOG.warn(String
							.format("Provisioned Throughput Exceeded - Retry Attempt %s",
									queryAttempts));

					Thread.sleep(2 ^ queryAttempts * backoffMillis);

					queryAttempts++;
				}
			} while (queryAttempts < 10 && result == null);

			if (result == null) {
				throw new Exception(String.format(
						"Unable to execute Query after %s attempts",
						queryAttempts));
			}

			lastKeyEvaluated = result.getLastEvaluatedKey();
		} while (lastKeyEvaluated != null);

		return output;
	}
}

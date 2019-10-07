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

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.aggregators.cache.AggregateCache;
import com.amazonaws.services.kinesis.aggregators.datastore.DynamoDataStore;
import com.amazonaws.services.kinesis.aggregators.datastore.DynamoQueryEngine.QueryKeyScope;
import com.amazonaws.services.kinesis.aggregators.datastore.IDataStore;
import com.amazonaws.services.kinesis.aggregators.exception.InvalidConfigurationException;
import com.amazonaws.services.kinesis.aggregators.exception.SerializationException;
import com.amazonaws.services.kinesis.aggregators.idempotency.DefaultIdempotencyCheck;
import com.amazonaws.services.kinesis.aggregators.idempotency.IIdempotencyCheck;
import com.amazonaws.services.kinesis.aggregators.metrics.CloudWatchMetricsEmitter;
import com.amazonaws.services.kinesis.aggregators.metrics.IMetricsEmitter;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.io.IDataExtractor;
import com.amazonaws.services.kinesis.model.Record;

/**
 * StreamAggregator is the main implementation of the Kinesis Aggregators
 * framework. It provides the ability to create dynamic aggregations in Dynamo
 * DB for data being streamed through Kinesis. Objects are aggregated on the
 * basis of the unique values contained in the Aggregate Label by Date, storing
 * event counts. Additionally, by configuring a set of summary values on the
 * StreamAggregator with AggregatorType set to SUM, an additional data element
 * is aggregated tracking the total value observed for each element of the
 * stream.Data in DynamoDB is aggregated on the basis of the configured
 * TimeHorizon, from granularity of SECOND to FOREVER.
 * 
 * @author meyersi
 */
public class StreamAggregator implements IStreamAggregator {
	public static final String AWSApplication = "AmazonKinesisAggregators";

	public static final String version = ".9.2.7.3";

	/**
	 * The default column name for the aggregated value, if none is provided.
	 */
	public static final String DEFAULT_AGGREGATE_COLUMN = "aggregatedValue";

	/**
	 * The default attribute name for the date value of an aggregate, if none is
	 * provided.
	 */
	public static final String DEFAULT_DATE_VALUE = "dateValue";

	/**
	 * The default attribute name for the count of events observed for an
	 * aggregate value and date, if none is provided.
	 */
	public static final String EVENT_COUNT = "eventCount";

	/**
	 * The attribute name for the time horizon marker
	 */
	public static final String TIME_HORIZON_ATTR = "timeHorizonType";

	/**
	 * The attribute name used for the last write sequence value in the table.
	 */
	public static final String LAST_WRITE_SEQ = "lastWriteSeq";

	/**
	 * The attribute name used for the timestamp of the update of the aggregate.
	 */
	public static final String LAST_WRITE_TIME = "lastWriteTime";

	/**
	 * The attribute used to refer to the partition key
	 */
	public static final String REF_PARTITION_KEY = "__partition_key";

	/**
	 * The attribute used to refer to the event sequence number
	 */
	public static final String REF_SEQUENCE = "__sequence";

	public static final SimpleDateFormat dateFormatter = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	protected String namespace;

	private KinesisClientLibConfiguration config;

	private String environment;

	private AmazonDynamoDB dynamoClient;

	private AmazonKinesisClient kinesisClient;

	private InventoryModel inventory;

	protected String tableName;

	protected boolean withTimeHierarchy = false;

	protected List<TimeHorizon> timeHorizons = new ArrayList<>();

	protected AggregatorType aggregatorType = AggregatorType.COUNT;

	protected long readCapacity;

	protected long writeCapacity;

	protected final String streamName;

	protected final String applicationName;

	protected String shardId = null;

	private boolean isFirstShardWorker = false;

	private final Log LOG = LogFactory.getLog(StreamAggregator.class);

	private Region region = null;

	protected AggregateCache cache;

	protected boolean online = false;

	protected String lowSeq;

	protected BigInteger highSeq = null;

	protected long start;

	private IDataExtractor dataExtractor;

	private IDataStore dataStore;

	private IIdempotencyCheck idempotencyCheck = new DefaultIdempotencyCheck();

	private IMetricsEmitter metricsEmitter;

	private boolean raiseExceptionOnDataExtractionErrors = true;

	private int ignoredRecordsBelowHWM = 0;

	private boolean publishMetrics = false;

	/**
	 * Copy Constructor
	 * 
	 * @param template
	 */
	public StreamAggregator(StreamAggregator template) throws Exception {
		this.streamName = template.streamName;
		this.applicationName = template.applicationName;
		this.namespace = template.namespace;
		this.config = template.config;
		this.dataExtractor = template.dataExtractor.copy();
		this.withDataStore(template.getDataStore());
		this.withAggregatorType(template.aggregatorType);
		this.withRaiseExceptionOnDataExtractionErrors(template.raiseExceptionOnDataExtractionErrors);
		this.withStorageCapacity(template.readCapacity, template.writeCapacity);
		this.withTableName(template.tableName);
		this.withTimeHorizon(template.timeHorizons);
		this.withIdempotencyCheck(template.idempotencyCheck);
		if (template.publishMetrics) {
			this.publishMetrics = true;
			this.metricsEmitter = template.metricsEmitter;
		}
	}

	public StreamAggregator(String streamName, String applicationName,
			String namespace, KinesisClientLibConfiguration config,
			IDataExtractor dataExtractor) {
		this.streamName = streamName;
		this.applicationName = applicationName;
		this.namespace = namespace;
		this.config = config;
		this.dataExtractor = dataExtractor;
	}

	public void checkpoint() throws Exception {
		cache.flush();
		lowSeq = null;

		// update the worker inventory showing progress to the last sequence
		// value
		inventory.update(this.streamName, this.applicationName, this.namespace,
				this.shardId, this.lowSeq, this.highSeq.toString(),
				System.currentTimeMillis(), InventoryModel.STATE.RUNNING);

		// warn and reset if there were any ignored records
		if (ignoredRecordsBelowHWM > 0) {
			logWarn(String
					.format("Processed %s records which were ignored due to being below the current processing HWM",
							ignoredRecordsBelowHWM));
			ignoredRecordsBelowHWM = 0;
		}

		LOG.debug("Aggregator Checkpoint for Shard " + this.shardId
				+ " Complete");
	}

	/*
	 * builder methods
	 */
	public StreamAggregator withStorageCapacity(Long readCapacity,
			Long writeCapacity) {
		if (readCapacity != null)
			this.readCapacity = readCapacity;
		if (writeCapacity != null)
			this.writeCapacity = writeCapacity;

		return this;
	}

	private void logInfo(String message) {
		LOG.info("[" + this.shardId + "] " + message);
	}

	private void logWarn(String message) {
		LOG.warn("[" + this.shardId + "] " + message);
	}

	private void logWarn(String message, Exception e) {
		LOG.warn("[" + this.shardId + "] " + message);
		LOG.error(e);
	}

	public void initialize(String shardId) throws Exception {
		// Set System properties to allow entity expansion of unlimited items in
		// response documents from AWS API
		//
		// see https://blogs.oracle.com/joew/entry/jdk_7u45_aws_issue_123 for
		// more information
		System.setProperty("entityExpansionLimit", "0");
		System.setProperty("jdk.xml.entityExpansionLimit", "0");

		this.shardId = shardId;

		// establish we are running on the lowest shard on the basis of hash
		// range
		AmazonKinesisClient kinesisClient = new AmazonKinesisClient(
				this.config.getKinesisCredentialsProvider());
		if (this.config.getRegionName() != null) {
			region = Region.getRegion(Regions.fromName(this.config
					.getRegionName()));
			kinesisClient.setRegion(region);
		}

		try {
			if (this.shardId.equals(StreamAggregatorUtils.getFirstShardName(
					kinesisClient, this.config.getStreamName()))) {
				this.isFirstShardWorker = true;
				logInfo("Aggregator taking Primary Thread Responsibility");
			}
		} catch (Exception e) {
			logWarn("Unable to establish if Worker Thread is Primary");
		}

		validateConfig();

		// set the default aggregator type
		if (this.aggregatorType == null) {
			this.aggregatorType = AggregatorType.COUNT;
		}

		if (this.dataExtractor == null)
			throw new InvalidConfigurationException(
					"Unable to create Aggregator Instance without a configured IDataStore");

		// set the aggregator type on the data extractor
		this.dataExtractor.setAggregatorType(this.aggregatorType);
		this.dataExtractor.validate();

		// create connections to dynamo and kinesis
		ClientConfiguration clientConfig = new ClientConfiguration()
				.withSocketTimeout(60000);
		this.dynamoClient = new AmazonDynamoDBAsyncClient(
				this.config.getDynamoDBCredentialsProvider(), clientConfig);
		if (region != null)
			this.dynamoClient.setRegion(region);
		// localhost
		this.dynamoClient.setEndpoint("http://dynamodb:8000");
		this.kinesisClient = new AmazonKinesisClient(
				this.config.getKinesisCredentialsProvider());
		if (region != null)
			this.kinesisClient.setRegion(region);
		this.kinesisClient.setEndpoint("http://kinesis:4567");

		inventory = new InventoryModel(this.dynamoClient);

		// get the latest sequence number checkpointed for this named aggregator
		// on this shard
		InventoryStatus lastUpdate = inventory.getLastUpdate(this.streamName,
				this.applicationName, this.namespace, this.shardId);
		if (lastUpdate != null && lastUpdate.getHighSeq() != null) {
			// set the current high sequence to the last high sequence
			this.highSeq = new BigInteger(lastUpdate.getHighSeq());
		}

		// log that we are now starting up
		inventory.update(this.streamName, this.applicationName, this.namespace,
				this.shardId, null, null, System.currentTimeMillis(),
				InventoryModel.STATE.STARTING);

		// set the table name we will use for aggregated values
		if (this.tableName == null) {
			this.tableName = StreamAggregatorUtils.getTableName(
					config.getApplicationName(), this.getNamespace());
		}

		if (this.environment != null && !this.environment.equals(""))
			this.tableName = String.format("%s.%s", this.environment,
					this.tableName);

		// resolve the basic data being aggregated
		String labelColumn = StreamAggregatorUtils.methodToColumn(dataExtractor
				.getAggregateLabelName());
		String dateColumn = dataExtractor.getDateValueName() == null ? DEFAULT_DATE_VALUE
				: dataExtractor.getDateValueName();

		// configure the default dynamo data store
		if (this.dataStore == null) {
			this.dataStore = new DynamoDataStore(this.dynamoClient,
					this.kinesisClient, this.aggregatorType, this.streamName,
					this.tableName, labelColumn, dateColumn)
					.withStorageCapacity(this.readCapacity, this.writeCapacity);
			this.dataStore.setRegion(region);
		}
		this.dataStore.initialise();

		// configure the cache so it can do its work
		cache = new AggregateCache(this.shardId)
				.withCredentials(this.config.getKinesisCredentialsProvider())
				.withAggregateType(this.aggregatorType)
				.withTableName(this.tableName).withLabelColumn(labelColumn)
				.withDateColumn(dateColumn).withDataStore(this.dataStore);

		// create a cloudwatch client for the cache to publish against if needed
		if (this.publishMetrics && this.metricsEmitter == null) {
			this.metricsEmitter = new CloudWatchMetricsEmitter(this.tableName,
					this.config.getCloudWatchCredentialsProvider());
		}

		if (this.metricsEmitter != null) {
			if (this.config.getRegionName() != null)
				this.metricsEmitter.setRegion(region);
		}
		// add the metrics publisher to the cache if we are bound to the lowest
		// shard
		if (this.metricsEmitter != null) {
			cache.withMetricsEmitter(this.metricsEmitter);
		}
		cache.initialise();

		// set the user agent
		StringBuilder userAgent = new StringBuilder(
				ClientConfiguration.DEFAULT_USER_AGENT);
		userAgent.append(" ");
		userAgent.append(this.AWSApplication);
		userAgent.append("/");
		userAgent.append(this.version);
		this.config.getKinesisClientConfiguration().setUserAgent(
				userAgent.toString());

		// log startup state
		StringBuffer sb = new StringBuffer();
		for (TimeHorizon t : timeHorizons) {
			sb.append(String.format("%s,", t.name()));
		}
		sb.deleteCharAt(sb.length() - 1);

		logInfo(String
				.format("Amazon Kinesis Stream Aggregator Online\nStream: %s\nApplication: %s\nNamespace: %s\nWorker: %s\nGranularity: %s\nContent Extracted With: %s",
						streamName, applicationName, this.namespace,
						this.config.getWorkerIdentifier(), sb.toString(),
						dataExtractor.getClass().getName()));
		if (this.highSeq != null)
			logInfo(String.format("Processing Data from Seq: %s", this.highSeq));
		online = true;
	}

	private void validateConfig() throws Exception {
		// this would only be null if the containing worker IRecordProcessor has
		// not called initialise()
		if (this.shardId == null) {
			throw new Exception(
					"Aggregator Not Online - Call Initialise to establish System State on Shard");
		}

		// default to Hourly granularity if the customer has not configured it
		if (this.timeHorizons == null) {
			withTimeHorizon(TimeHorizon.HOUR);
		}
	}

	/**
	 * Add a single
	 * {@link com.amazonaws.services.kinesis.aggregators.TimeHorizon} to the
	 * configuration of the Aggregator
	 * 
	 * @param horizon
	 *            TimeHorizon value to be used for aggregated data
	 * @return
	 */
	public StreamAggregator withTimeHorizon(TimeHorizon horizon) {
		if (this.timeHorizons == null)
			this.timeHorizons = new ArrayList<>();

		this.timeHorizons.add(horizon);

		return this;
	}

	/**
	 * Add a set of
	 * {@link com.amazonaws.services.kinesis.aggregators.TimeHorizon} values to
	 * the configuration of the Aggregator
	 * 
	 * @param horizon
	 *            TimeHorizon value to be used for aggregated data
	 * @return
	 */
	public StreamAggregator withTimeHorizon(List<TimeHorizon> horizons) {
		if (this.timeHorizons == null) {
			this.timeHorizons = horizons;
		} else {
			this.timeHorizons.addAll(horizons);
		}

		return this;
	}

	/**
	 * Add a set of
	 * {@link com.amazonaws.services.kinesis.aggregators.TimeHorizon} values to
	 * the configuration of the Aggregator
	 * 
	 * @param horizon
	 *            TimeHorizon value to be used for aggregated data
	 * @return
	 */
	public StreamAggregator withTimeHorizon(TimeHorizon... horizons) {
		if (this.timeHorizons == null)
			this.timeHorizons = new ArrayList<>();

		for (TimeHorizon t : horizons) {
			this.timeHorizons.add(t);
		}

		return this;
	}

	/**
	 * Set the name of the data store in Dynamo DB for the Aggregated Data
	 * 
	 * @param tableName
	 *            The table name to use for data storage
	 * @return
	 */
	public StreamAggregator withTableName(String tableName) {
		this.tableName = tableName;
		return this;
	}

	/**
	 * Select an explicit
	 * {@link com.amazonaws.servies.kinesis.aggregators.AggregatorType} for the
	 * Aggregator. Default is COUNT
	 * 
	 * @param t
	 *            The Aggregator Type to use
	 * @return
	 */
	public StreamAggregator withAggregatorType(AggregatorType t) {
		if (t != null) {
			this.aggregatorType = t;
		}
		return this;
	}

	/**
	 * Override the default behaviour of an Aggregator to fail when the data
	 * stream cannot be deserialised. When setting this value to 'true', then
	 * the Aggregator stream will be able to deal with bad data that cannot be
	 * aggregated, and will simply continue working
	 * 
	 * @param bool
	 *            Boolean indicating whether to fail when bad data is received
	 *            on the stream and cannot be deserialised
	 * @return
	 */
	public StreamAggregator withRaiseExceptionOnDataExtractionErrors(
			boolean bool) {
		this.raiseExceptionOnDataExtractionErrors = bool;
		return this;
	}

	/**
	 * Should we publish CloudWatch metrics for all captured data?
	 * 
	 * @param bool
	 * @return
	 */
	public StreamAggregator withCloudWatchMetrics() {
		this.publishMetrics = true;
		return this;
	}

	/**
	 * Allow configuring a non-Default data store
	 * 
	 * @param dataStore
	 * @return
	 */
	public StreamAggregator withDataStore(IDataStore dataStore) {
		if (dataStore != null) {
			this.dataStore = dataStore;
		}
		return this;
	}

	/**
	 * Allow configuring a non-Default metrics emitter
	 * 
	 * @param metricsEmitter
	 * @return
	 */
	public StreamAggregator withMetricsEmitter(IMetricsEmitter metricsEmitter) {
		if (metricsEmitter != null) {
			this.metricsEmitter = metricsEmitter;
		}
		return this;
	}

	/**
	 * Allow configuring a non-Default idempotency check
	 * 
	 * @param idempotencyCheck
	 * @return
	 */
	public StreamAggregator withIdempotencyCheck(
			IIdempotencyCheck idempotencyCheck) {
		if (idempotencyCheck != null) {
			this.idempotencyCheck = idempotencyCheck;
		}
		return this;
	}

	public StreamAggregator withEnvironment(EnvironmentType environment) {
		this.environment = environment.name();
		return this;
	}

	public StreamAggregator withEnvironment(String environment) {
		this.environment = environment;
		return this;
	}

	/* Simple property accessors */
	public String getNamespace() {
		return this.namespace;
	}

	public IDataExtractor getDataExtractor() {
		return this.dataExtractor;
	}

	public IDataStore getDataStore() {
		return this.dataStore;
	}

	public String getTableName() {
		return this.tableName;
	}

	public String getLabelAttribute() {
		return this.dataExtractor.getAggregateLabelName();
	}

	public String getDateAttribute() {
		return this.dataExtractor.getDateValueName();
	}

	public AggregatorType getAggregatorType() {
		return this.aggregatorType;
	}

	public long getReadCapacity() {
		return this.readCapacity;
	}

	public long getWriteCapacity() {
		return this.writeCapacity;
	}

	public List<TimeHorizon> getTimeHorizon() {
		return this.timeHorizons;
	}

	/**
	 * Shut down an aggregator and mark its state as Stopped in the Inventory
	 * Table
	 * 
	 * @param flushState
	 *            Should the aggregator clear it's pending updates prior to
	 *            shutting down
	 * @param withState
	 *            Final status for the aggregator
	 * @throws Exception
	 */
	public void shutdown() throws Exception {
		shutdown(true);
	}

	public void shutdown(boolean flushState) throws Exception {
		shutdown(flushState, null);
	}

	public void shutdown(boolean flushState, InventoryModel.STATE withState)
			throws Exception {
		if (flushState)
			checkpoint();

		if (inventory != null)
			inventory.update(this.streamName, this.applicationName,
					this.namespace, this.shardId, null, null, System
							.currentTimeMillis(),
					withState == null ? InventoryModel.STATE.STOPPED
							: withState);

	}

	/**
	 * {@inheritDoc}
	 */
	public void aggregate(List<Record> records) throws Exception {
		List<InputEvent> events = new ArrayList<>();

		for (Record r : records) {
			events.add(new InputEvent(r));
		}

		aggregateEvents(events);
	}

	/**
	 * {@inheritDoc}
	 */
	public void aggregateEvents(List<InputEvent> events) throws Exception {
		start = System.currentTimeMillis();
		int aggregatedEventCount = 0;
		int aggregatedElementCount = 0;

		if (!online) {
			throw new Exception("Aggregator Not Initialised");
		}

		BigInteger thisSequence;
		List<AggregateData> extractedItems = null;
		Date eventDate = null;

		try {
			for (InputEvent event : events) {
				// reset extracted items
				extractedItems = null;

				if (event.getSequenceNumber() != null) {
					thisSequence = new BigInteger(event.getSequenceNumber());
					// ignore any records which are going backward with regard
					// to
					// the current hwm
					if (highSeq != null
							&& highSeq.compareTo(thisSequence) > 0) {
						ignoredRecordsBelowHWM++;
						continue;
					}
				}

				// set the low sequence if this is the first record received
				// after a flush
				if (lowSeq == null)
					lowSeq = event.getSequenceNumber();

				// high sequence is always the latest value
				highSeq = new BigInteger(event.getSequenceNumber());

				// extract the data from the input event
				try {
					extractedItems = dataExtractor.getData(event);
				} catch (SerializationException se) {
					// customer may have elected to suppress serialisation
					// errors if the stream is expected have heterogenous data
					// on it
					if (this.raiseExceptionOnDataExtractionErrors) {
						throw se;
					} else {
						logWarn(String.format(
								"Serialisation Exception Sequence %s Partition Key %s",
								event.getSequenceNumber(),
								event.getPartitionKey()), se);
					}
				}

				// data extractor may have returned multiple data elements, or
				// be empty if there were serialisation problems which are
				// suppressed
				if (extractedItems != null) {
					aggregatedEventCount++;

					for (AggregateData data : extractedItems) {
						// run the idempotency check
						if (!this.idempotencyCheck.doProcess(
								event.getPartitionKey(),
								event.getSequenceNumber(), data,
								event.getData())) {
							logInfo(String
									.format("Ignoring Event %s as it failed Idempotency Check",
											event.getPartitionKey()));
							continue;
						}

						aggregatedElementCount++;

						// if the data extractor didn't have a date value to
						// extract, then use the current time
						eventDate = data.getDate();
						if (eventDate == null) {
							eventDate = new Date(System.currentTimeMillis());
						}

						// generate the local updates, one per time horizon that
						// is requested
						for (TimeHorizon h : timeHorizons) {
							// atomically update the aggregate table with event
							// count or count + summaries
							cache.update(
									aggregatorType,
									data.getLabels(),
									(timeHorizons.size() > 1 ? h
											.getItemWithMultiValueFormat(eventDate)
											: h.getValue(eventDate)), h, event
											.getSequenceNumber(), 1, data
											.getSummaries(), dataExtractor
											.getSummaryConfig());
						}
					}
				}
			}

			logInfo(String
					.format("Aggregation Complete - %s Records and %s Elements in %s ms",
							aggregatedEventCount, aggregatedElementCount,
							(System.currentTimeMillis() - start)));
		} catch (SerializationException se) {
			shutdown(true, InventoryModel.STATE.SERIALISATION_ERROR);
			LOG.error(se);
			throw se;
		} catch (Exception e) {
			shutdown(true, InventoryModel.STATE.UNKNOWN_ERROR);
			LOG.error(e);
			throw e;
		}
	}

	/**
	 * Return the stored value for a label and date value at the configured time
	 * granularity
	 * 
	 * @param label
	 *            The Aggregated Label Value to get data for
	 * @param dateValue
	 *            The Date Value to obtain data from
	 * @param h
	 *            The Time Horizon to query
	 * @return
	 */
	public List<Map<String, AttributeValue>> queryValue(String label,
			Date dateValue, ComparisonOperator comp) throws Exception {
		if (!(this.dataStore instanceof DynamoDataStore)) {
			throw new Exception(
					"Unable to Query by Date unless Data Store is Dynamo DB");
		}

		if (comp != null && comp.equals(ComparisonOperator.BETWEEN)) {
			throw new InvalidConfigurationException(
					"Between Operator Not Supported");
		}

		return ((DynamoDataStore) this.dataStore).queryEngine().queryByKey(
				label, dateValue, comp);
	}

	/**
	 * Query all data in the data store for a given range of date values and
	 * time horizon
	 * 
	 * @param dateValue
	 *            The date to search relative to
	 * @param h
	 *            The Time Horizon to limit search to
	 * @param comp
	 *            The Comparison Operator to be applied to the dateValue, such
	 *            as 'equal' EQ or 'greater than' GT
	 * @return A list of data stored in Dynamo DB for the time range
	 * @throws Exception
	 */
	public List<Map<String, AttributeValue>> queryByDate(Date dateValue,
			TimeHorizon h, ComparisonOperator comp, int threads)
			throws Exception {
		if (!(this.dataStore instanceof DynamoDataStore)) {
			throw new Exception(
					"Unable to Query by Date unless Data Store is Dynamo DB");
		}

		if (comp.equals(ComparisonOperator.BETWEEN)) {
			throw new InvalidConfigurationException(
					"Between Operator Not Supported");
		}

		// resolve the query date based on if we are managing multiple time
		// values or a single
		String queryDate = null;
		if (this.timeHorizons.size() > 1) {
			queryDate = h.getItemWithMultiValueFormat(dateValue);
		} else {
			queryDate = h.getValue(dateValue);
		}

		// setup the query condition on date
		Map<String, Condition> conditions = new HashMap<>();
		Condition dateCondition = new Condition().withComparisonOperator(comp)
				.withAttributeValueList(new AttributeValue().withS(queryDate));
		conditions.put(this.dataExtractor.getDateValueName(), dateCondition);

		List<Map<String, AttributeValue>> items = ((DynamoDataStore) this.dataStore)
				.queryEngine().parallelQueryDate(
						this.dataExtractor.getDateValueName(), conditions,
						threads);

		return items;
	}

	public List<TableKeyStructure> parallelQueryKeys(QueryKeyScope scope,
			int threads) throws Exception {
		if (!(this.dataStore instanceof DynamoDataStore)) {
			throw new Exception(
					"Unable to Query Keys unless Data Store is Dynamo DB");
		}

		logInfo(String
				.format("Executing Unique Key Scan on %s with Scope %s using %s Threads",
						this.tableName, scope.toString(), threads));
		return ((DynamoDataStore) this.dataStore).queryEngine()
				.parallelQueryKeys(scope, threads);
	}
}

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
package com.amazonaws.services.kinesis.aggregators.cache;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.services.kinesis.aggregators.AggregatorType;
import com.amazonaws.services.kinesis.aggregators.EnvironmentType;
import com.amazonaws.services.kinesis.aggregators.LabelSet;
import com.amazonaws.services.kinesis.aggregators.TimeHorizon;
import com.amazonaws.services.kinesis.aggregators.datastore.AggregateAttributeModification;
import com.amazonaws.services.kinesis.aggregators.datastore.DynamoDataStore;
import com.amazonaws.services.kinesis.aggregators.datastore.IDataStore;
import com.amazonaws.services.kinesis.aggregators.exception.InvalidConfigurationException;
import com.amazonaws.services.kinesis.aggregators.metrics.IMetricsEmitter;
import com.amazonaws.services.kinesis.aggregators.summary.SummaryConfiguration;
import com.amazonaws.services.kinesis.aggregators.summary.SummaryElement;

/**
 * AggregateCache encapsulates the in-flight copy of aggregated data, which is
 * flushed to Dynamo DB when the aggregator checkpoints.
 */
/*
 * Flush and Update methods are not thread safe so are marked as synchronised.
 * Intended utilisation in an inherently multi-threaded environment will be with
 * multiple instances of an Aggregator, which will by definition generate
 * multiple instances of the cache, so this synchronisation should not be an
 * issue in practice
 */
public class AggregateCache {
    private String shardId;

    private String environment;

    private String streamName, tableName, labelName, dateName;

    private AWSCredentialsProvider credentials;

    private AggregatorType aggregatorType = AggregatorType.COUNT;

    private Map<UpdateKey, UpdateValue> pendingUpdates;

    private long reportUpdatesPendingCount = -1;

    private long warnUpdatesPendingCount = -1;

    private long forceCheckpointOnPendingUpdateCount = -1;

    private final int updateForceCheckpointFrequency = 3;

    private int forcedCount = 0;

    private final Log LOG = LogFactory.getLog(AggregateCache.class);

    private boolean online = false;

    private IMetricsEmitter metricsEmitter;

    private IDataStore dataStore = null;

    private Region region;

    public AggregateCache(String shardId) {
        this.shardId = shardId;
    }

    private void logInfo(String message) {
        LOG.info("[" + this.shardId + "] " + message);
    }

    private void logWarn(String message) {
        LOG.warn("[" + this.shardId + "] " + message);
    }

    /**
     * Configure the Aggregate Cache with its underlying data store.
     * 
     * @throws Exception
     */
    public void initialise() throws Exception {
        if (pendingUpdates == null) {
            pendingUpdates = new HashMap<>();
        }

        // configure the default dynamo data store
        if (this.dataStore == null) {
            this.dataStore = new DynamoDataStore(this.credentials, this.aggregatorType,
                    this.streamName, this.tableName, this.labelName, dateName).withStorageCapacity(
                    DynamoDataStore.DEFAULT_READ_CAPACITY, DynamoDataStore.DEFAULT_WRITE_CAPACITY);
            this.dataStore.setRegion(region);
        }
        this.dataStore.initialise();

        // set the checkpointing thresholds based on the current io throughputs
        setCheckpointForcingThresholds();

        this.online = true;
    }

    protected long getReportUpdatesPendingCount() {
        return reportUpdatesPendingCount;
    }

    protected long getWarnUpdatesPendingCount() {
        return warnUpdatesPendingCount;
    }

    protected long getForceCheckpointOnPendingUpdateCount() {
        return forceCheckpointOnPendingUpdateCount;
    }

    /* builder methods */
    public AggregateCache withEnvironment(EnvironmentType environment) {
        this.environment = environment.name();
        return this;
    }

    public AggregateCache withEnvironment(String environment) {
        this.environment = environment;
        return this;
    }

    public AggregateCache withTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public AggregateCache withStreamName(String streamName) {
        this.streamName = streamName;
        return this;
    }

    public AggregateCache withRegion(Region region) {
        this.region = region;
        return this;
    }

    public AggregateCache withLabelColumn(String labelColumn) {
        this.labelName = labelColumn;
        return this;
    }

    public AggregateCache withDateColumn(String dateColumn) {
        this.dateName = dateColumn;
        return this;
    }

    public AggregateCache withCredentials(AWSCredentialsProvider credentials) {
        this.credentials = credentials;
        return this;
    }

    public AggregateCache withAggregateType(AggregatorType type) {
        this.aggregatorType = type;
        return this;
    }

    public AggregateCache withMetricsEmitter(IMetricsEmitter metricsEmitter) {
        this.metricsEmitter = metricsEmitter;
        return this;
    }

    public AggregateCache withDataStore(IDataStore dataStore) {
        this.dataStore = dataStore;

        return this;
    }

    protected void setCheckpointForcingThresholds() throws Exception {
        // set the force checkpoint level @ 4 minutes of write capacity, warning
        // at half that, and info an half the warning threshold
        this.forceCheckpointOnPendingUpdateCount = this.dataStore.refreshForceCheckpointThresholds();
        this.warnUpdatesPendingCount = (long) Math.ceil(this.forceCheckpointOnPendingUpdateCount / 2);
        this.reportUpdatesPendingCount = (long) Math.ceil(this.warnUpdatesPendingCount / 2);
    }

    /**
     * Mechanism to update the pending update set with new summary values, based
     * upon new events being consumed and calculated with the indicated
     * calculation.
     * 
     * @param aggregatorType The type of Aggregator that the cache is being used
     *        with
     * @param fieldLabel The label value on which data will be aggregated
     * @param dateValue The date value on which data will be aggregated
     * @param seq The sequence number of the underlying Kinesis record which
     *        generated the update
     * @param countIncrement The increment of count for the item
     * @param summedIncrements The set of summary values to be added to the
     *        aggregate
     * @param calculationConfig The configuration of what types of summaries
     *        should be applied to the summed fields
     * @throws Exception
     */
    /*
     * This method is synchronised to prevent any issues where the consumer has
     * not implemented the aggregator=>worker mapping in a threadsafe manner.
     * Using the internal IRecordProcessor and IRecordProcessorFactory, we
     * generate new instances of the aggregator per shard worker thread.
     * However, a customer may allocate a single aggregator to multiple workers,
     * and while this will be slower, at least the data in the backing store
     * will be correct
     */
    public synchronized void update(final AggregatorType aggregatorType, final LabelSet fieldLabel,
            final String dateValue, final TimeHorizon timeHorizon, final String seq,
            final Integer countIncrement, final Map<String, Double> summedIncrements,
            SummaryConfiguration calculationConfig) throws Exception {
        // lazy validate the configuration
        if (!online)
            initialise();

        // get the payload for the current label value to be updated
        UpdateKey key = new UpdateKey(fieldLabel, this.dateName, dateValue, timeHorizon);
        UpdateValue payload = pendingUpdates.get(key);
        if (payload == null) {
            payload = new UpdateValue();
        }

        // always update the count
        payload.incrementCount(countIncrement);

        // process summary updates based on the summary configuration
        if (aggregatorType.equals(AggregatorType.SUM)) {
            // process all the requested calculations
            for (String s : calculationConfig.getItemSet()) {
                for (SummaryElement e : calculationConfig.getRequestedCalculations(s)) {
                    // be tolerant that not every summary item may be present on
                    // every extracted item
                    if (summedIncrements.containsKey(s)) {
                        payload.updateSummary(e.getAttributeAlias(),
                                summedIncrements.get(e.getStreamDataElement()), e);
                    } else {
                        logWarn(String.format(
                                "Summary Item '%s' not found in Extracted Data - Ignoring", s));
                    }
                }
            }
        }

        // update the last write sequence and time
        payload.lastWrite(seq, System.currentTimeMillis());

        // write the updates back
        pendingUpdates.put(key, payload);

        // put some nags into the log to remind an implementer to checkpoint
        // periodically
        if (pendingUpdates.size() % reportUpdatesPendingCount == 0) {
            logInfo(String.format("%s Pending Aggregates to be flushed", pendingUpdates.size()));
        }

        if (pendingUpdates.size() > warnUpdatesPendingCount) {
            logWarn(String.format("Warning - %s Pending Aggregates - Checkpoint NOW",
                    pendingUpdates.size()));
        }

        // checkpoint manually at the force threshold to prevent the aggregator
        // falling over
        if (pendingUpdates.size() > forceCheckpointOnPendingUpdateCount) {
            logWarn(String.format(
                    "Forcing checkpoint at %s Aggregates to avoid KCL Worker Disconnect - please ensure you have checkpointed the enclosing IRecordProcessor",
                    pendingUpdates.size()));
            flush();

            forcedCount++;

            if (forcedCount % updateForceCheckpointFrequency == 0) {
                // allow the system to refresh the force checkpoint thresholds
                // periodically
                setCheckpointForcingThresholds();
            }
        }
    }

    public UpdateValue get(UpdateKey key) {
        return pendingUpdates.get(key);
    }

    protected IDataStore getDataStore() {
        return this.dataStore;
    }

    /**
     * Flush the state of all pending in memory updates to Dynamo DB.
     * 
     * @throws Exception
     */
    /*
     * See comments on aggregate() as to why this method is synchronised
     */
    public synchronized void flush() throws Exception {
        long startTime = System.currentTimeMillis();
        Map<UpdateKey, Map<String, AggregateAttributeModification>> dataModifications = this.dataStore.write(pendingUpdates);
        logInfo(String.format("Cache Flushed %s modifications in %sms", this.pendingUpdates.size(),
                (System.currentTimeMillis() - startTime)));

        // publish the cloudwatch metrics
        if (this.metricsEmitter != null)
            try {
                startTime = System.currentTimeMillis();
                this.metricsEmitter.emit(dataModifications);
                logInfo(String.format("Instrumentation Dispatched to Metrics Service in %sms",
                        (System.currentTimeMillis() - startTime)));
            } catch (Exception e) {
                // log the error but do not fail
                LOG.error("Metrics Emitter Exception - Aggregate Cache will NOT terminate");
                LOG.error(e);
            }

        pendingUpdates = new HashMap<>();
    }
}

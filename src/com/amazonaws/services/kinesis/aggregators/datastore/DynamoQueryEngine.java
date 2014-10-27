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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.kinesis.aggregators.StreamAggregator;
import com.amazonaws.services.kinesis.aggregators.StreamAggregatorUtils;
import com.amazonaws.services.kinesis.aggregators.TableKeyStructure;

public class DynamoQueryEngine {
    private final Log LOG = LogFactory.getLog(DynamoQueryEngine.class);

    private AmazonDynamoDB dynamoClient;

    private String tableName, labelAttribute, dateAttribute;

    protected final int BACKOFF_MILLIS = 10;

    public DynamoQueryEngine(AmazonDynamoDB dynamoClient, String tableName, String labelAttribute,
            String dateAttribute) {
        this.dynamoClient = dynamoClient;
        this.tableName = tableName;
        this.labelAttribute = labelAttribute;
        this.dateAttribute = dateAttribute;
    }

    public enum QueryKeyScope {
        HashKey, HashAndRangeKey;
    }

    public List<TableKeyStructure> parallelQueryKeys(QueryKeyScope scope, int threads)
            throws Exception {
        List<ParallelKeyScanWorker> workers = new ArrayList<>();
        Collection<Future<?>> workerStatus = new ArrayList<>();
        List<TableKeyStructure> output = new ArrayList<>();
        int totalResultsProcessed = 0;

        // set up the executor thread pool
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        // create workers for each segment that we need to do queries against
        for (int i = 0; i < threads; i++) {
            ParallelKeyScanWorker worker = new ParallelKeyScanWorker(this.tableName, i, threads,
                    scope, this.labelAttribute, this.dateAttribute);
            workers.add(worker);
            workerStatus.add(executor.submit(worker));
        }

        for (Future<?> f : workerStatus) {
            f.get();
        }
        executor.shutdown();

        for (ParallelKeyScanWorker w : workers) {
            // throw any exceptions the worker incurred
            w.throwExceptions();

            if (w.getResultCount() > 0) {
                output.addAll(w.getOutput());
            }

            totalResultsProcessed += w.getResultsProcessed();
        }

        LOG.info(String.format("Key Extraction Complete - Processed %s Key Items",
                totalResultsProcessed));

        return output;
    }

    public List<Map<String, AttributeValue>> queryByKey(String label, Date dateValue,
            ComparisonOperator operator) throws Exception {
        if (dateValue != null && !operator.equals(ComparisonOperator.EQ)) {
            String dateAsString = StreamAggregator.dateFormatter.format(dateValue);

            LOG.info(String.format("Issuing Hash/Range Query for %s - %s", label, dateAsString));

            // range query
            Map<String, Condition> keyConditions = new HashMap<>();

            // hash key
            Condition c = new Condition().withAttributeValueList(new AttributeValue().withS(label)).withComparisonOperator(
                    ComparisonOperator.EQ);
            keyConditions.put(this.labelAttribute, c);

            // range key
            c = new Condition().withAttributeValueList(new AttributeValue().withS(dateAsString)).withComparisonOperator(
                    operator);
            keyConditions.put(this.dateAttribute, c);

            QueryRequest req = new QueryRequest().withTableName(this.tableName).withKeyConditions(
                    keyConditions);

            return DynamoUtils.queryUntilDone(dynamoClient, req, BACKOFF_MILLIS);
        } else {
            if (dateValue == null) {
                LOG.info(String.format("Issuing Hash Key Only Query for %s", label));

                // hash key only query
                Map<String, Condition> keyConditions = new HashMap<>();
                Condition c = new Condition().withAttributeValueList(
                        new AttributeValue().withS(label)).withComparisonOperator(
                        ComparisonOperator.EQ);
                keyConditions.put(this.labelAttribute, c);
                QueryRequest req = new QueryRequest().withTableName(this.tableName).withKeyConditions(
                        keyConditions);

                return DynamoUtils.queryUntilDone(dynamoClient, req, BACKOFF_MILLIS);
            } else {
                String dateAsString = StreamAggregator.dateFormatter.format(dateValue);

                LOG.info(String.format("Performing exact Hash/Range Lookup for %s - %s", label,
                        dateAsString));

                // exact key lookup
                List<Map<String, AttributeValue>> output = new ArrayList<>();
                Map<String, AttributeValue> keyMap = new HashMap<>();
                keyMap.put(this.labelAttribute, new AttributeValue().withS(label));
                keyMap.put(this.dateAttribute, new AttributeValue().withS(dateAsString));
                GetItemRequest req = new GetItemRequest().withTableName(this.tableName).withKey(
                        keyMap);
                output.add(this.dynamoClient.getItem(req).getItem());
                return output;
            }
        }
    }

    private class ParallelKeyScanWorker implements Runnable {
        List<TableKeyStructure> output = new ArrayList<>();

        private String tableName, hashKey, rangeKey;

        private QueryKeyScope scope;

        private int workerInstance, threads;

        private int resultsProcessed = 0;

        private Exception exception;

        public ParallelKeyScanWorker(String tableName, int workerInstance, int threads,
                QueryKeyScope scope, String hashKey, String rangeKey) {
            this.tableName = tableName;
            this.workerInstance = workerInstance;
            this.hashKey = hashKey;
            this.rangeKey = rangeKey;
            this.threads = threads;
            this.scope = scope;
        }

        public int getResultCount() {
            if (this.output == null) {
                return 0;
            } else {
                return this.output.size();
            }
        }

        public int getResultsProcessed() {
            return this.resultsProcessed;
        }

        public void throwExceptions() throws Exception {
            if (this.exception != null) {
                throw this.exception;
            }
        }

        @Override
        public void run() {
            ScanRequest scanRequest = new ScanRequest().withTableName(this.tableName).withAttributesToGet(
                    this.hashKey).withSegment(this.workerInstance).withTotalSegments(threads);
            Map<String, Set<String>> deduplicated = new HashMap<>();
            Set<String> rangeValues = null;
            Map<String, AttributeValue> lastKeyEvaluated = null;
            int scanAttempts = 0;
            int limit = -1;
            boolean returnedResults = false;
            String lastLabel = null;
            int uniqueLabels = 0;

            do {
                ScanResult result = null;

                // set query limits, to optimise for skip scan or for hash/range
                // query with no limit
                if (this.scope.equals(QueryKeyScope.HashKey)) {
                    if (uniqueLabels > 0 && uniqueLabels == resultsProcessed) {
                        // remove the query limit if every row being returned is
                        // unique
                        limit = -1;
                    } else {
                        // set a limit of twice the number of uniques, so we can
                        // get a larger result set as we go
                        if (uniqueLabels == 0) {
                            limit = 100;
                        } else {
                            limit = uniqueLabels * 2;
                        }

                        // reset the unique labels so it doesn't grow without
                        // limit
                        uniqueLabels = 0;
                    }
                } else {
                    scanRequest.withAttributesToGet(this.rangeKey);
                }

                do {
                    try {
                        // set the limit if we have one
                        if (limit != -1) {
                            scanRequest.withLimit(limit);
                        }
                        result = dynamoClient.scan(scanRequest.withExclusiveStartKey(lastKeyEvaluated));

                        if (result.getItems().size() > 0) {
                            returnedResults = true;
                        } else {
                            returnedResults = false;
                        }
                    } catch (ProvisionedThroughputExceededException e) {
                        LOG.warn(String.format(
                                "Provisioned Throughput Exceeded - Retry Attempt %s", scanAttempts));

                        // back off
                        try {
                            Thread.sleep(2 ^ scanAttempts * BACKOFF_MILLIS);
                        } catch (InterruptedException interrupted) {
                            this.exception = interrupted;
                            return;
                        }
                        scanAttempts++;
                    }
                } while (scanAttempts < 10 && result == null);

                if (result == null) {
                    this.exception = new Exception(String.format(
                            "Unable to execute Scan after %s attempts", scanAttempts));
                    return;
                }

                // process the results, creating a deduplicated map/set of
                // hash/range keys
                String labelValue = null;
                if (returnedResults) {
                    for (Map<String, AttributeValue> map : result.getItems()) {
                        resultsProcessed++;

                        labelValue = map.get(this.hashKey).getS();

                        // only enter the label value into the hash once
                        if (scope.equals(QueryKeyScope.HashKey)) {
                            if (!labelValue.equals(lastLabel) || lastLabel == null) {
                                deduplicated.put(labelValue, null);
                                lastLabel = labelValue;
                                uniqueLabels++;
                            }
                        } else {
                            if (deduplicated.containsKey(labelValue)) {
                                rangeValues = deduplicated.get(labelValue);
                            } else {
                                rangeValues = new HashSet<String>();
                            }

                            rangeValues.add(map.get(this.rangeKey).getS());

                            deduplicated.put(labelValue, rangeValues);
                        }
                    }

                    // set the last evaluated key. if we have processed a bunch
                    // of data and are not at the end of the result set, then
                    // we'll force a skip forward on date, to eliminate
                    // continued processing of high cardinality hash values
                    if (this.scope.equals(QueryKeyScope.HashKey)
                            && result.getLastEvaluatedKey() != null) {
                        // skip scan
                        lastKeyEvaluated = new HashMap<>();
                        lastKeyEvaluated.put(this.hashKey, new AttributeValue().withS(labelValue));
                        lastKeyEvaluated.put(this.rangeKey,
                                new AttributeValue().withS("4000-01-01 00:00:00"));
                    } else {
                        lastKeyEvaluated = result.getLastEvaluatedKey();
                    }
                } else {
                    lastKeyEvaluated = null;
                }
            } while (lastKeyEvaluated != null);

            if (this.scope.equals(QueryKeyScope.HashKey)) {
                LOG.debug(String.format("Worker %s extracted %s results", this.workerInstance,
                        deduplicated.size()));
            } else {
                LOG.debug(String.format(
                        "Worker %s deduplicated %s results, creating distinct set of %s keys",
                        this.workerInstance, resultsProcessed, deduplicated.size()));
            }

            this.output = new ArrayList<>();
            if (deduplicated.size() > 0) {
                for (String s : deduplicated.keySet()) {
                    TableKeyStructure t = new TableKeyStructure(this.hashKey, s, this.rangeKey);

                    if (scope.equals(QueryKeyScope.HashAndRangeKey)) {
                        for (String rangeValue : deduplicated.get(s)) {
                            t.withDateValue(rangeValue);
                        }
                    }

                    output.add(t);
                }
            }
        }

        public List<TableKeyStructure> getOutput() {
            return this.output;
        }
    }

    private class ParallelDateQueryWorker implements Runnable {
        private List<Map<String, AttributeValue>> output = new ArrayList<>();

        private int start, range;

        private String tableName, indexName, labelAttribute, dateAttribute;

        private Map<String, Condition> conditions;

        private Exception exception;

        public void throwException() throws Exception {
            if (this.exception != null)
                throw this.exception;
        }

        public ParallelDateQueryWorker(String tableName, String indexName, int start, int range,
                Map<String, Condition> conditions, String labelAttribute, String dateAttribute) {
            this.tableName = tableName;
            this.indexName = indexName;
            this.start = start;
            this.range = range;
            this.conditions = conditions;
            this.labelAttribute = labelAttribute;
            this.dateAttribute = dateAttribute;
        }

        @Override
        public void run() {
            for (int i = this.start; i < this.start + this.range; i++) {
                Condition c = new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(
                        new AttributeValue().withN("" + i));
                this.conditions.put(DynamoDataStore.SCATTER_PREFIX_ATTRIBUTE, c);
                QueryRequest req = new QueryRequest().withIndexName(this.indexName).withTableName(
                        this.tableName).withKeyConditions(this.conditions);

                Map<String, AttributeValue> lastKeyEvaluated = null;
                do {
                    int queryAttempts = 0;
                    QueryResult result = null;

                    do {
                        try {
                            result = dynamoClient.query(req).withLastEvaluatedKey(lastKeyEvaluated);

                            this.output.addAll(result.getItems());
                        } catch (ProvisionedThroughputExceededException e) {
                            LOG.warn(String.format(
                                    "Provisioned Throughput Exceeded - Retry Attempt %s",
                                    queryAttempts));

                            try {
                                Thread.sleep(2 ^ queryAttempts * BACKOFF_MILLIS);
                            } catch (InterruptedException interrupted) {
                                this.exception = interrupted;
                                return;
                            }
                            queryAttempts++;
                        }
                    } while (queryAttempts < 10 && result == null);

                    if (result == null) {
                        this.exception = new Exception(String.format(
                                "Unable to execute Query after %s attempts", queryAttempts));
                        return;
                    }

                    lastKeyEvaluated = result.getLastEvaluatedKey();
                } while (lastKeyEvaluated != null);
            }
        }

        /**
         * This method takes a query result on the GSI, and returns a
         * deduplicated set of hash/range values in the form of a Map indexed by
         * the label value, with one entry per date value
         * 
         * @return
         */
        public KeysAndAttributes dedupExtractKeys() {
            KeysAndAttributes keys = new KeysAndAttributes();

            Map<String, Set<AttributeValue>> out = new HashMap<>();
            String labelValue = null;
            AttributeValue dateValue = null;
            Set<AttributeValue> values;

            // go through all of the output
            for (Map<String, AttributeValue> map : this.output) {
                // process each attribute
                for (String s : map.keySet()) {
                    // grab the label and date values
                    if (s.equals(this.labelAttribute)) {
                        labelValue = map.get(s).getS();
                    } else if (s.equals(this.dateAttribute)) {
                        dateValue = map.get(s);
                    }
                }

                // get the current set of date values for the label, or create a
                // new one
                if (!out.containsKey(labelValue)) {
                    values = new HashSet<>();
                } else {
                    values = out.get(labelValue);
                }

                // add the current date value to the set of all date values for
                // the label
                values.add(dateValue);

                // write back the map of label to date values
                out.put(labelValue, values);
            }

            // now pivot the deduplicated output key set into a
            // KeysAndAttributes object
            for (final String s : out.keySet()) {
                for (final AttributeValue value : out.get(s)) {
                    keys.withKeys(new HashMap<String, AttributeValue>() {
                        {
                            put(labelAttribute, new AttributeValue().withS(s));
                            put(dateAttribute, value);
                        }
                    });
                }
            }

            return keys;
        }
    }

    private List<Map<String, AttributeValue>> batchGetDataByKeys(final String tableName,
            final KeysAndAttributes keys) {
        Map<String, KeysAndAttributes> requestMap = new HashMap<>();
        keys.setConsistentRead(true);
        requestMap.put(tableName, keys);
        BatchGetItemResult result = dynamoClient.batchGetItem(new BatchGetItemRequest(requestMap));

        return result.getResponses().get(this.tableName);
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, AttributeValue>> parallelQueryDate(String onAttribute,
            Map<String, Condition> conditions, int threads) throws Exception {
        // figure out the range of scatter prefix values we are going to assign
        // to each thread
        int range = (DynamoDataStore.SCATTER_WIDTH / threads) + 1;
        List<ParallelDateQueryWorker> workers = new ArrayList<>();
        Collection<Future<?>> workerStatus = new ArrayList<>();
        List<Map<String, AttributeValue>> output = new ArrayList<>();

        // set up the executor thread pool
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        // determine which index we should work with
        String indexName;
        if (onAttribute.equals(StreamAggregator.LAST_WRITE_SEQ)) {
            indexName = StreamAggregatorUtils.getLastWriteSeqIndexName(this.tableName);
        } else {
            indexName = StreamAggregatorUtils.getDateDimensionIndexName(this.tableName, onAttribute);
        }

        StringBuilder conditionString = new StringBuilder();
        for (String s : conditions.keySet()) {
            conditionString.append(String.format("%s %s %s,", s,
                    conditions.get(s).getComparisonOperator(),
                    conditions.get(s).getAttributeValueList().get(0)));
        }

        LOG.info(String.format(
                "Querying %s with %s Threads on %s (Conditions: %s)",
                indexName,
                threads,
                onAttribute,
                conditionString.length() > 0 ? conditionString.substring(0,
                        conditionString.length() - 1).toString() : "None"));

        // create workers for each segment that we need to do queries against
        for (int i = 0; i < DynamoDataStore.SCATTER_WIDTH; i++) {
            if (i == 0 || i % range == 0) {
                ParallelDateQueryWorker worker = new ParallelDateQueryWorker(this.tableName,
                        indexName, i, range, conditions, this.labelAttribute, this.dateAttribute);
                workers.add(worker);
                workerStatus.add(executor.submit(worker));
            }
        }
        for (Future<?> f : workerStatus) {
            f.get();
        }
        executor.shutdown();

        // collect the results from the workers
        int outputCounter = 0;
        KeysAndAttributes queryKeys;

        for (ParallelDateQueryWorker w : workers) {
            // throw any exceptions that the worker handled
            w.throwException();

            queryKeys = new KeysAndAttributes();

            // generate a set of KeysAndAttributes from the deduplicated output
            // map of table keys
            KeysAndAttributes workerKeys = w.dedupExtractKeys();

            // break the returned KeysAndAttributes up into batches of 25 and
            // query for them
            if (workerKeys != null && workerKeys.getKeys() != null) {
                for (Map<String, AttributeValue> key : workerKeys.getKeys()) {
                    queryKeys.withKeys(key);

                    outputCounter++;

                    if (outputCounter % 25 == 0) {
                        output.addAll(batchGetDataByKeys(this.tableName, queryKeys));
                        queryKeys = new KeysAndAttributes();
                    }
                }
                // one final query for anything < mod(25)=0
                output.addAll(batchGetDataByKeys(this.tableName, queryKeys));
            }
        }

        return output;
    }
}

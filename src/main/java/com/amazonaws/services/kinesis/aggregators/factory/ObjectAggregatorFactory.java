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
package com.amazonaws.services.kinesis.aggregators.factory;

import java.util.Arrays;
import java.util.List;

import com.amazonaws.services.kinesis.aggregators.AggregatorType;
import com.amazonaws.services.kinesis.aggregators.StreamAggregator;
import com.amazonaws.services.kinesis.aggregators.TimeHorizon;
import com.amazonaws.services.kinesis.aggregators.annotations.AnnotationProcessor;
import com.amazonaws.services.kinesis.aggregators.datastore.DynamoDataStore;
import com.amazonaws.services.kinesis.aggregators.datastore.IDataStore;
import com.amazonaws.services.kinesis.aggregators.metrics.CloudWatchMetricsEmitter;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.io.IDataExtractor;
import com.amazonaws.services.kinesis.io.ObjectExtractor;

public class ObjectAggregatorFactory {
    private ObjectAggregatorFactory() {
    }

    /**
     * Create a new Aggregator for Object Serialised Data based upon a Class
     * which is configured using Annotations from the base class.
     * 
     * @param streamName The Stream Name that the Aggregator is receiving data
     *        from.
     * @param appName The Application Name that an Aggregator is part of.
     * @param config The Kinesis Client Library Configuration to inherit
     *        credentials and connectivity to the database from.
     * @param clazz The annotated class to use for configuration of the
     *        aggregator
     * @return A Stream Aggregator which can process object serialised data
     * @throws Exception
     */
    public static final StreamAggregator newInstance(String streamName, String appName,
            KinesisClientLibConfiguration config, Class clazz) throws Exception {
        AnnotationProcessor p = new AnnotationProcessor(clazz);
        ObjectExtractor dataExtractor = new ObjectExtractor(p.getLabelMethodNames(), clazz).withDateMethod(p.getDateMethodName());

        dataExtractor.withSummaryConfig(p.getSummaryConfig());
        //dataExtractor.withSummaryMethods(new ArrayList<>(p.getSummaryMethods().keySet()));

        StreamAggregator agg = new StreamAggregator(streamName, appName, p.getNamespace(), config,
                dataExtractor).withTimeHorizon(p.getTimeHorizon()).withAggregatorType(p.getType()).withRaiseExceptionOnDataExtractionErrors(
                p.shouldFailOnDataExtractionErrors());

        // configure metrics service on the aggregator if it's been
        // configured
        if (p.shouldEmitMetrics()
                || (p.getMetricsEmitter() != null && !p.getMetricsEmitter().equals(
                        CloudWatchMetricsEmitter.class))) {
            if (p.getMetricsEmitter() != null) {
                agg.withMetricsEmitter(p.getMetricsEmitter().newInstance());
            } else {
                agg.withCloudWatchMetrics();
            }
        }

        // create a new instance of the Data Store if one has been
        // configured. Currently we only support pluggable data stores that
        // are configured via their environment or have self defined
        // configuration models: only no args public constructors can be
        // called
        if (p.getDataStore() != null && !p.getDataStore().equals(DynamoDataStore.class)) {
            agg.withDataStore((IDataStore) p.getDataStore().newInstance());
        }

        return agg;
    }

    /**
     * Create a new Aggregator for data which is object serialised on the stream
     * using Jackson JSON Serialisation.
     * 
     * @param streamName The Stream Name that the Aggregator is receiving data
     *        from.
     * @param appName The Application Name that an Aggregator is part of.
     * @param config The Kinesis Client Library Configuration to inherit
     *        credentials and connectivity to the database from.
     * @param namespace The namespace used to separate this Aggregator's output
     *        data from other Aggregated data
     * @param timeHorizon The Time Horizon value to use for the granularity of
     *        the Aggregated data
     * @param aggregatorType The type of Aggregator to create. Default is COUNT.
     * @param clazz The base class to use as a Transfer Object for the data
     *        stream.
     * @param labelMethods The method on the base class to use to obtain the
     *        label for aggregation.
     * @param dateMethod The method on the object which should be used to
     *        establish the time. If NULL then the client receive time will be
     *        used.
     * @param summaryMethods List of summary method names or expressions to be
     *        used when the AggregatorType is SUM, as secondary aggregated data
     *        points
     * @return A Stream Aggregator which can process object serialised data
     * @throws Exception
     */
    public static final StreamAggregator newInstance(String streamName, String appName,
            KinesisClientLibConfiguration config, String namespace, TimeHorizon timeHorizon,
            AggregatorType aggregatorType, Class clazz, List<String> labelMethods,
            String dateMethod, List<String> summaryMethods) throws Exception {
        return newInstance(streamName, appName, config, namespace,
                Arrays.asList(new TimeHorizon[] { timeHorizon }), aggregatorType, clazz,
                labelMethods, dateMethod, summaryMethods);
    }

    /**
     * Create a new Aggregator for data which is object serialised on the stream
     * using Jackson JSON Serialisation.
     * 
     * @param streamName The Stream Name that the Aggregator is receiving data
     *        from.
     * @param appName The Application Name that an Aggregator is part of.
     * @param config The Kinesis Client Library Configuration to inherit
     *        credentials and connectivity to the database from.
     * @param namespace The namespace used to separate this Aggregator's output
     *        data from other Aggregated data.
     * @param timeHorizons The list of Time Horizon values to use the
     *        aggregator. Data will be automatically managed at ALL of the
     *        requested granularities using a prefixed namespace on dates.
     * @param aggregatorType The type of Aggregator to create. Default is COUNT.
     * @param clazz The base class to use as a Transfer Object for the data
     *        stream.
     * @param labelMethods The methods on the base class to use to obtain the
     *        label for aggregation.
     * @param dateMethod The method on the object which should be used to
     *        establish the time. If NULL then the client receive time will be
     *        used.
     * @param summaryMethods List of summary method names or expressions to be
     *        used when the AggregatorType is SUM, as secondary aggregated data
     *        points.
     * @return A Stream Aggregator which can process object serialised data.
     * @return
     * @throws Exception
     */
    public static final StreamAggregator newInstance(String streamName, String appName,
            KinesisClientLibConfiguration config, String namespace, List<TimeHorizon> timeHorizons,
            AggregatorType aggregatorType, Class clazz, List<String> labelMethods,
            String dateMethod, List<String> summaryMethods) throws Exception {
        IDataExtractor dataExtractor = new ObjectExtractor(labelMethods, clazz).withDateMethod(
                dateMethod).withSummaryMethods(summaryMethods);
        dataExtractor.setAggregatorType(aggregatorType);
        return new StreamAggregator(streamName, appName, namespace, config, dataExtractor).withTimeHorizon(
                timeHorizons).withAggregatorType(aggregatorType);
    }
}

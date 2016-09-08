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
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.io.IDataExtractor;
import com.amazonaws.services.kinesis.io.JsonDataExtractor;

public class JsonAggregatorFactory {
    private JsonAggregatorFactory() {
    }

    /**
     * Creates an Aggregator for data that is formatted as JSON Strings on the
     * Kinesis Stream.
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
     * @param labelAttributes The attribute name in the JSON document which
     *        should be used as the label value for Aggregation
     * @param dateAttribute The attribute name in the JSON document which should
     *        be used for the time element of the Aggregation. If NULL then the
     *        client receive time will be used.
     * @param dateFormat The format of the dateAttribute, if String based dates
     *        are used. This should follow {@link java.text.SimpleDateFormat}
     *        convention.
     * @param summaryAttributes List of attributes or expressions on attributes
     *        which should be used for summary aggregation.
     * @return A Stream Aggregator which can process JSON data containing the
     *         indicated attributes.
     * @throws Exception
     */
    public static final StreamAggregator newInstance(String streamName, String appName,
            KinesisClientLibConfiguration config, String namespace, TimeHorizon timeHorizon,
            AggregatorType aggregatorType, List<String> labelAttributes, String dateAttribute,
            String dateFormat, List<String> summaryAttributes) throws Exception {
        return newInstance(streamName, appName, config, namespace,
                Arrays.asList(new TimeHorizon[] { timeHorizon }), aggregatorType, labelAttributes,
                dateAttribute, dateFormat, summaryAttributes);
    }

    /**
     * Creates an Aggregator for data that is formatted as JSON Strings on the
     * Kinesis Stream.
     * 
     * @param streamName The Stream Name that the Aggregator is receiving data
     *        from.
     * @param appName The Application Name that an Aggregator is part of.
     * @param workerId The worker ID hosting the Aggregator.
     * @param config The Kinesis Client Library Configuration to inherit
     *        credentials and connectivity to the database from.
     * @param namespace The namespace used to separate this Aggregator's output
     *        data from other Aggregated data.
     * @param timeHorizons The list of Time Horizon values to use the
     *        aggregator. Data will be automatically managed at ALL of the
     *        requested granularities using a prefixed namespace on dates.
     * @param aggregatorType The type of Aggregator to create. Default is COUNT.
     * @param labelAttributes The attribute name in the JSON document which
     *        should be used as the label value for Aggregation.
     * @param dateAttribute The attribute name in the JSON document which should
     *        be used for the time element of the Aggregation. If NULL then the
     *        client receive time will be used.
     * @param dateFormat The format of the dateAttribute, if String based dates
     *        are used. This should follow {@link java.text.SimpleDateFormat}
     *        convention.
     * @param summaryAttributes List of attributes or expressions on attributes
     *        which should be used for summary aggregation.
     * @return A Stream Aggregator which can process JSON data containing the
     *         indicated attributes.
     * @throws Exception
     */
    public static final StreamAggregator newInstance(String streamName, String appName,
            KinesisClientLibConfiguration config, String namespace, List<TimeHorizon> timeHorizons,
            AggregatorType aggregatorType, List<String> labelAttributes, String dateAttribute,
            String dateFormat, List<String> summaryAttributes) throws Exception {
        IDataExtractor dataExtractor = new JsonDataExtractor(labelAttributes).withDateValueAttribute(
                dateAttribute).withSummaryAttributes(summaryAttributes).withDateFormat(dateFormat);
        dataExtractor.setAggregatorType(aggregatorType);
        return new StreamAggregator(streamName, appName, namespace, config, dataExtractor).withTimeHorizon(
                timeHorizons).withAggregatorType(aggregatorType);
    }
}

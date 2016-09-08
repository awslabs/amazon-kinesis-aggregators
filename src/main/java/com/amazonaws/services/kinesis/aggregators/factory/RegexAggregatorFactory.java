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
import com.amazonaws.services.kinesis.io.RegexDataExtractor;
import com.amazonaws.services.kinesis.io.StringDataExtractor;

/**
 * Factory Class used for generating Aggregators which use Regular Expressions
 * to extract data from the Kinesis Stream.
 */
public class RegexAggregatorFactory {
    private RegexAggregatorFactory() {
    }

    /**
     * Factory Method which generates a Regular Expression based Aggregator for
     * a number of Time Horizons
     * 
     * @param streamName The name of the Stream to aggregate against.
     * @param appName The application name to associate with the aggregator.
     * @param config The Kinesis Configuration used for the containing worker.
     * @param namespace The namespace to associate with the aggregated data.
     * @param timeHorizon The time horizons on which to aggregate data.
     * @param aggregatorType The type of aggregator to create.
     * @param regularExpression The regular expression used to extract data from
     *        the Kinesis Stream via Character Classes
     * @param labelIndicies The index of the extracted data to be used as the
     *        aggregation label
     * @param dateIndex The index of the extracted data to be used as the time
     *        value
     * @param dateFormat The format of the data which represents the event time
     *        when shipped as a String
     * @param summaryIndicies The indicies or Summary Expressions on indicies
     *        which contain summary values to be aggregated
     * @return
     * @throws Exception
     */
    public static final StreamAggregator newInstance(String streamName, String appName,
            KinesisClientLibConfiguration config, String namespace, List<TimeHorizon> timeHorizons,
            AggregatorType aggregatorType, String regularExpression, List<Integer> labelIndicies,
            String labelAttributeAlias, int dateIndex, String dateFormat, String dateAlias,
            List<Object> summaryIndicies) throws Exception {
        StringDataExtractor dataExtractor = new RegexDataExtractor(regularExpression, labelIndicies).withDateValueIndex(
                dateIndex).withDateFormat(dateFormat).withSummaryIndicies(summaryIndicies);
        dataExtractor.setAggregatorType(aggregatorType);

        if (labelAttributeAlias != null && !labelAttributeAlias.equals("")) {
            dataExtractor.withLabelAttributeAlias(labelAttributeAlias);
        }
        if (dateAlias != null && !dateAlias.equals("")) {
            dataExtractor.withDateAttributeAlias(dateAlias);
        }
        return new StreamAggregator(streamName, appName, namespace, config, dataExtractor).withTimeHorizon(
                timeHorizons).withAggregatorType(aggregatorType);
    }

    /**
     * Factory Method which generates a Regular Expression based Aggregator for
     * a single Time Horizon
     * 
     * @param streamName
     * @param appName
     * @param config
     * @param namespace
     * @param timeHorizon
     * @param aggregatorType
     * @param regularExpression
     * @param labelIndicies
     * @param dateIndex
     * @param dateFormat
     * @param summaryIndicies
     * @return
     * @throws Exception
     */
    public static final StreamAggregator newInstance(String streamName, String appName,
            KinesisClientLibConfiguration config, String namespace, TimeHorizon timeHorizon,
            AggregatorType aggregatorType, String regularExpression, List<Integer> labelIndicies,
            String labelAttributeAlias, int dateIndex, String dateFormat, String dateAlias,
            List<Object> summaryIndicies) throws Exception {
        return newInstance(streamName, appName, config, namespace,
                Arrays.asList(new TimeHorizon[] { timeHorizon }), aggregatorType,
                regularExpression, labelIndicies, labelAttributeAlias, dateIndex, dateFormat,
                dateAlias, summaryIndicies);
    }
}

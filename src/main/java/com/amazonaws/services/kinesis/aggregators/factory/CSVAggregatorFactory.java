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
import com.amazonaws.services.kinesis.aggregators.summary.SummaryCalculation;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.io.CsvDataExtractor;
import com.amazonaws.services.kinesis.io.StringDataExtractor;

/**
 * Factory Class used for generating Aggregators which support CSV data on the
 * Kinesis Stream.
 */
public class CSVAggregatorFactory {
    private CSVAggregatorFactory() {
    }

    /**
     * Factory Method to generate a new Aggregator for CSV Data.
     * 
     * @param streamName The name of the Stream to aggregate against.
     * @param appName The application name to associate with the aggregator.
     * @param config The Kinesis Configuration used for the containing worker.
     * @param namespace The namespace to associate with the aggregated data.
     * @param timeHorizon The time horizons on which to aggregate data.
     * @param aggregatorType The type of aggregator to create.
     * @param delimiter The character delimiter for data on the stream.
     * @param labelIndicies The position of the field in the stream data which
     *        should be used to aggregate data.
     * @param dateIndex The position of the field which includes a date item
     *        used to aggregate data by the timeHorizon. Values can be in String
     *        format if dateFormat is supplied, or in epoch seconds.
     * @param dateFormat The format of the date item, if provided as a String
     * @param summaryIndicies The list of field positions, or expressions using
     *        a {@link SummaryCalculation} against the field positions. For
     *        example, simple summaries might have a list of '0,1,2' or when
     *        expressions are used, a list of 'min(0),sum(1),max(2)'.
     * @return Returns a new CSV Aggregator.
     * @throws Exception
     */
    public static final StreamAggregator newInstance(String streamName, String appName,
            KinesisClientLibConfiguration config, String namespace, TimeHorizon timeHorizon,
            AggregatorType aggregatorType, String delimiter, List<Integer> labelIndicies,
            String labelAttributeAlias, int dateIndex, String dateFormat, String dateAlias,
            List<Object> summaryIndicies) throws Exception {
        return newInstance(streamName, appName, config, namespace,
                Arrays.asList(new TimeHorizon[] { timeHorizon }), aggregatorType, delimiter,
                labelIndicies, labelAttributeAlias, dateIndex, dateFormat, dateAlias,
                summaryIndicies);
    }

    public static final StreamAggregator newInstance(String streamName, String appName,
            KinesisClientLibConfiguration config, String namespace, List<TimeHorizon> timeHorizons,
            AggregatorType aggregatorType, String delimiter, List<Integer> labelIndicies,
            String labelAttributeAlias, int dateIndex, String dateFormat, String dateAlias,
            List<Object> summaryIndicies) throws Exception {
        StringDataExtractor dataExtractor = new CsvDataExtractor(labelIndicies).withDelimiter(
                delimiter).withDateValueIndex(dateIndex).withDateFormat(dateFormat).withSummaryIndicies(
                summaryIndicies);
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
}

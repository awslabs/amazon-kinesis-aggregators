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
package com.amazonaws.services.kinesis.io;

import java.util.List;

import com.amazonaws.services.kinesis.aggregators.AggregateData;
import com.amazonaws.services.kinesis.aggregators.AggregatorType;
import com.amazonaws.services.kinesis.aggregators.InputEvent;
import com.amazonaws.services.kinesis.aggregators.exception.SerializationException;
import com.amazonaws.services.kinesis.aggregators.summary.SummaryConfiguration;

/**
 * Interface which allows for pluggable data extractors for different types of
 * stream data. Aggregators use IDataExtractor to interoperate between the
 * stream data format and the internal format required for Aggregation.
 * IDataExtractors likely use IKinesisSerialisers to read and write to and from
 * the stream
 */
public interface IDataExtractor {
    /**
     * Get the name of the element which represents the unique ID for the event,
     * if there is one
     */
    public String getUniqueIdName();

    /**
     * Get the name of the label value to be extracted.
     * 
     * @return
     */
    public String getAggregateLabelName();

    /**
     * Get the name of the date value to be extracted.
     * 
     * @return
     */
    public String getDateValueName();

    /**
     * Extract one or more aggregatable items from a Kinesis Record.
     * 
     * @param event The Kinesis Record data from which we want to extract data.
     * @return A list of ExtractedData elements which have been resolved from
     *         the input data.
     * @throws SerializationException
     */
    public List<AggregateData> getData(InputEvent event) throws SerializationException;

    /**
     * Set the type of aggregator which contains this IDataExtractor. Used to
     * boost efficiency in that the Extractor will not extract summary items for
     * COUNT based Aggregator integration.
     * 
     * @param type
     */
    public void setAggregatorType(AggregatorType type);

    /**
     * Validate that the extractor is well formed.
     * 
     * @throws Exception
     */
    public void validate() throws Exception;

    /**
     * Get the summary configuration that is driving data extraction against the
     * data stream.
     * 
     * @return
     */
    public SummaryConfiguration getSummaryConfig();

    public IDataExtractor copy() throws Exception;
}

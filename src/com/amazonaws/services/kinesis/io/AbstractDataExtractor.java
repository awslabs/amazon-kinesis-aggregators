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
 * Abstract class which provides some helper methods for creating IDataExtractor
 * classes.
 */
public abstract class AbstractDataExtractor implements IDataExtractor {
    protected AggregatorType aggregatorType = AggregatorType.COUNT;

    protected SummaryConfiguration summaryConfig = new SummaryConfiguration();

    public abstract String getAggregateLabelName();

    public abstract String getDateValueName();

    public abstract List<AggregateData> getData(InputEvent event) throws SerializationException;

    public abstract void validate() throws Exception;

    public AggregatorType getAggregatorType() {
        return this.aggregatorType;
    }

    public void setAggregatorType(AggregatorType type) {
        this.aggregatorType = type;
    }

    public SummaryConfiguration getSummaryConfig() {
        return this.summaryConfig;
    }
}

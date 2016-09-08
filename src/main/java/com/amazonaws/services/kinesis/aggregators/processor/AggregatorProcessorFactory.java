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
package com.amazonaws.services.kinesis.aggregators.processor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.aggregators.AggregatorGroup;
import com.amazonaws.services.kinesis.aggregators.StreamAggregator;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

/**
 * Simple factory class to generate a standalone Kinesis Aggregator
 * IRecordProcessor for the application
 */
public class AggregatorProcessorFactory implements IRecordProcessorFactory {
    private AggregatorGroup aggregators;

    private final Log LOG = LogFactory.getLog(AggregatorProcessorFactory.class);

    private AggregatorProcessorFactory() {
    }

    /**
     * Create a Processor Factory that will create an Aggregator Processor which
     * wraps the indicated Aggregator
     * 
     * @param agg
     */
    public AggregatorProcessorFactory(StreamAggregator agg) {
        this.aggregators = new AggregatorGroup();
        this.aggregators.registerAggregator(agg);
    }

    public AggregatorProcessorFactory(AggregatorGroup group) {
        this.aggregators = group;
    }

    /**
     * {@inheritDoc}
     */
    public IRecordProcessor createProcessor() {
        try {
            // every time we create a new processor instance, we have to embed a
            // new instance of the AggregatorGroup, to eliminate any thread
            // contention
            return new AggregatorProcessor(new AggregatorGroup(this.aggregators));
        } catch (Exception e) {
            LOG.error(e);
            return null;
        }
    }
}

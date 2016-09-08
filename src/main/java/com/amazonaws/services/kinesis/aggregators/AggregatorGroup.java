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

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.model.Record;

/**
 * Class which provides a simple automation around a number of aggregators.
 * Register any number of aggregators with the container, and then call all of
 * the registered aggregators aggregate and checkpoint methods through this
 * simple proxy
 */
public class AggregatorGroup implements IStreamAggregator {
    List<StreamAggregator> aggregators = new ArrayList<>();

    public AggregatorGroup() {
    }

    public AggregatorGroup(AggregatorGroup template) throws Exception {
        // create a new aggregator group from all of the aggregators this one
        // encapsulates, by instantiating new aggregators with their copy
        // constructors
        for (StreamAggregator agg : template.aggregators) {
            this.registerAggregator(new StreamAggregator(agg));
        }
    }

    public void registerAggregator(StreamAggregator agg) {
        this.aggregators.add(agg);
    }

    public List<StreamAggregator> getAggregators() {
        return this.aggregators;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void aggregate(List<Record> records) throws Exception {
        for (IStreamAggregator agg : aggregators) {
            agg.aggregate(records);
        }
    }

    public void aggregateEvents(List<InputEvent> events) throws Exception {
        for (IStreamAggregator agg : aggregators) {
            agg.aggregateEvents(events);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void checkpoint() throws Exception {
        for (IStreamAggregator agg : aggregators) {
            agg.checkpoint();
        }
    }

    @Override
    public void initialize(String shardId) throws Exception {
        for (IStreamAggregator agg : aggregators) {
            agg.initialize(shardId);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown(boolean flushState) throws Exception {
        for (IStreamAggregator agg : aggregators) {
            agg.shutdown(flushState);
        }
    }

    /**
     * N/A - use getTableNames()
     */
    @Override
    public String getTableName() {
        return null;
    }

    public List<String> getTableNames() {
        List<String> out = new ArrayList<>(this.aggregators.size());

        for (IStreamAggregator i : this.aggregators) {
            out.add(i.getTableName());
        }

        return out;
    }
}

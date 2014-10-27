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

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.regions.Region;
import com.amazonaws.services.kinesis.aggregators.StreamAggregator;
import com.amazonaws.services.kinesis.aggregators.cache.UpdateKey;
import com.amazonaws.services.kinesis.aggregators.cache.UpdateValue;
import com.amazonaws.services.kinesis.aggregators.summary.SummaryCalculation;

public class DevNullDataStore implements IDataStore {

    @Override
    public Map<UpdateKey, Map<String, AggregateAttributeModification>> write(
            Map<UpdateKey, UpdateValue> data) throws Exception {
        /*
         * Simply return a remapped set of what the caller sent us - their
         * values are the final values for this data store
         */
        Map<UpdateKey, Map<String, AggregateAttributeModification>> output = new HashMap<>();

        for (UpdateKey key : data.keySet()) {
            Map<String, AggregateAttributeModification> updates = new HashMap<>();

            updates.put(StreamAggregator.EVENT_COUNT, new AggregateAttributeModification(
                    StreamAggregator.EVENT_COUNT, StreamAggregator.EVENT_COUNT,
                    data.get(key).getAggregateCount(), SummaryCalculation.SUM));

            for (String value : data.get(key).getSummaryValues().keySet()) {
                updates.put(value, data.get(key).getSummary(value));
            }

            output.put(key, updates);
        }

        return output;
    }

    @Override
    public void initialise() throws Exception {
    }

    @Override
    public long refreshForceCheckpointThresholds() throws Exception {
        return 0;
    }

    @Override
    public void setRegion(Region region) {
    }
}

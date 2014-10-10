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

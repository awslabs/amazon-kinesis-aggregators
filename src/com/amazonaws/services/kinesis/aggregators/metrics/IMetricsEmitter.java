package com.amazonaws.services.kinesis.aggregators.metrics;

import java.util.Map;

import com.amazonaws.regions.Region;
import com.amazonaws.services.kinesis.aggregators.cache.UpdateKey;
import com.amazonaws.services.kinesis.aggregators.datastore.AggregateAttributeModification;

/**
 * Interface for providing classes which can write to metrics services. It
 * receives the output of the IDataStore modifications, and applies the data to
 * the metrics service
 */
public interface IMetricsEmitter {
    /**
     * Emit a new set of metrics to the metrics service
     * 
     * @param metricData Input Data to be intrumented
     * @throws Exception
     */
    public void emit(Map<UpdateKey, Map<String, AggregateAttributeModification>> metricData)
            throws Exception;

    /**
     * Method called to indicate the Region of the metrics service
     * 
     * @param region
     */
    public void setRegion(Region region);
}

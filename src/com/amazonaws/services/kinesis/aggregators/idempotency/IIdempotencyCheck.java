package com.amazonaws.services.kinesis.aggregators.idempotency;

import com.amazonaws.services.kinesis.aggregators.AggregateData;

/**
 * Interface which allows for the configuration of an Idempotency Check, which
 * will conditionally select whether a record should be processed
 */
public interface IIdempotencyCheck {
    /**
     * Should the input event be processed by the configured Aggregators?
     * 
     * @param event The Deserialised and resolved data element
     * @return True for process, False for don't
     */
    public boolean doProcess(String partitionKey, String sequenceNumber, AggregateData dataElement,
            byte[] originalData);
}

package com.amazonaws.services.kinesis.aggregators.idempotency;

import com.amazonaws.services.kinesis.aggregators.AggregateData;

/**
 * Default implementation of an Idempotency Check. Always returns True - that an
 * input element should be processed
 */
public class DefaultIdempotencyCheck implements IIdempotencyCheck {
    public DefaultIdempotencyCheck() {
    }

    public boolean doProcess(String partitionKey, String sequenceNumber, AggregateData dataElement,
            byte[] originalData) {
        return true;
    }
}

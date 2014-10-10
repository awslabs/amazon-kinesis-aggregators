package com.amazonaws.services.kinesis.aggregators.metrics;

public class MetricsEmitterThrottledException extends Exception {
    public MetricsEmitterThrottledException() {
        super();
    }

    public MetricsEmitterThrottledException(String message) {
        super(message);
    }

    public MetricsEmitterThrottledException(Exception e) {
        super(e);
    }
}

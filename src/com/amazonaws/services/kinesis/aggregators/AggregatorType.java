package com.amazonaws.services.kinesis.aggregators;

/**
 * Types of Aggregators supported by the Kinesis Aggregator Framework.
 */
public enum AggregatorType {
    /**
     * Count Aggregators maintain only an Event Count observed for the indicated
     * {@link com.amazonaws.services.kinesis.aggregators.TimeHorizon}
     */
    COUNT,
    /**
     * Sum Aggregators maintain an Event Count, plus a set of summary values for
     * data indicated on the stream as being a summary value. Summary Values can
     * be any of
     * {@link com.amazonaws.services.kinesis.aggregators.SummaryCalculation}
     */
    SUM;
}

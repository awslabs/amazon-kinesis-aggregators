package com.amazonaws.services.kinesis.aggregators.exception;

import com.amazonaws.services.kinesis.aggregators.summary.SummaryCalculation;

/**
 * Exception thrown when a summary value is indicated that is not one of
 * {@link com.amazonaws.services.kinesis.aggregators.summary.SummaryCalculation}
 */
public class UnsupportedCalculationException extends Exception {
    private String message;

    public UnsupportedCalculationException(String message) {
        super();
        this.message = message;
    }

    @Override
    public String getMessage() {
        return this.message;
    }
}

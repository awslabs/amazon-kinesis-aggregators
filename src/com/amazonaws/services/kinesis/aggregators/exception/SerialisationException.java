package com.amazonaws.services.kinesis.aggregators.exception;

@SuppressWarnings("serial")
public class SerialisationException extends Exception {
    private String message;

    public SerialisationException(String message) {
        super(message);
    }

    public SerialisationException(Exception e) {
        super(e);
    }

    public SerialisationException(String message, Exception e) {
        super(message, e);
    }

    @Override
    public String getMessage() {
        return this.message;
    }
}

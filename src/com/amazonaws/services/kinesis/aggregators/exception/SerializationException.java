package com.amazonaws.services.kinesis.aggregators.exception;

@SuppressWarnings("serial")
public class SerializationException extends Exception {
    private String message;

    public SerializationException(String message) {
        super(message);
    }

    public SerializationException(Exception e) {
        super(e);
    }

    public SerializationException(String message, Exception e) {
        super(message, e);
    }

    @Override
    public String getMessage() {
        return this.message;
    }
}

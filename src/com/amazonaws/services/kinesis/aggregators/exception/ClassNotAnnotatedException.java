package com.amazonaws.services.kinesis.aggregators.exception;

@SuppressWarnings("serial")
public class ClassNotAnnotatedException extends Exception {
    private String message;

    public ClassNotAnnotatedException(Exception e) {
        super(e);
    }

    public ClassNotAnnotatedException(String message, Exception e) {
        super(message, e);
    }

    public ClassNotAnnotatedException(String message) {
        super(message);
    }

    @Override
    public String getMessage() {
        return this.message;
    }
}

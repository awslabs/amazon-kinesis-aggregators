package com.amazonaws.services.kinesis.aggregators.exception;

public class InvalidConfigurationException extends Exception {

    public InvalidConfigurationException() {
    }

    public InvalidConfigurationException(String message) {
        super(message);
        // TODO Auto-generated constructor stub
    }

    public InvalidConfigurationException(Throwable cause) {
        super(cause);
    }

    public InvalidConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidConfigurationException(String message, Throwable cause,
            boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

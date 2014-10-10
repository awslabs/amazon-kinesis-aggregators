package com.amazonaws.services.kinesis.aggregators.configuration;

public enum DataExtractor {
    JSON("com.amazonaws.services.kinesis.io.JsonDataExtractor"), CSV(
            "com.amazonaws.services.kinesis.io.CsvDataExtractor"), OBJECT(
            "com.amazonaws.services.kinesis.io.ObjectExtractor"), REGEX(
            "com.amazonaws.services.kinesis.io.RegexDataExtractor");

    private DataExtractor(String linkedClass) {
        this.linkedClass = linkedClass;
    }

    private String linkedClass;

    public String getLinkedClass() {
        return this.linkedClass;
    }
}

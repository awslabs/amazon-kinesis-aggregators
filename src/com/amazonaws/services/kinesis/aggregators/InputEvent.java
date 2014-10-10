package com.amazonaws.services.kinesis.aggregators;

import com.amazonaws.services.kinesis.model.Record;

public class InputEvent {
    private String sequenceNumber;

    private String partitionKey;

    private byte[] data;

    public InputEvent(Record record) {
        this.sequenceNumber = record.getSequenceNumber();
        this.partitionKey = record.getPartitionKey();
        this.data = record.getData().array();
    }

    public InputEvent withSequence(String sequence) {
        this.sequenceNumber = sequence;
        return this;
    }

    public InputEvent withPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
        return this;
    }

    public String getSequenceNumber() {
        return sequenceNumber;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public byte[] getData() {
        return data;
    }
}

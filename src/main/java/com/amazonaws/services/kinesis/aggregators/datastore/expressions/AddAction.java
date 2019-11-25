package com.amazonaws.services.kinesis.aggregators.datastore.expressions;

public class AddAction implements IAction {
    private String key, value;
    public AddAction(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String getAction() {
        return "ADD";
    }

    @Override
    public String toString() {
        return key + " " + value;
    }
}

package com.amazonaws.services.kinesis.aggregators.datastore.expressions;


public class PutAction implements IAction {
    private String key, value;
    public PutAction(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getAction() {
        return "PUT";
    }

    @Override
    public String toString() {
        return "%s %s".format(key, value);
    }
}

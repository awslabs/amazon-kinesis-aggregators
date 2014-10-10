package com.amazonaws.services.kinesis.aggregators;

import java.util.HashSet;
import java.util.Set;

public class TableKeyStructure {
    private String labelAttributeName, labelAttributeValue, dateAttributeName;

    private Set<String> dateValues;

    public TableKeyStructure() {
    }

    public TableKeyStructure(String labelAttributeName, String labelAttributeValue,
            String dateAttributeName) {
        this.labelAttributeName = labelAttributeName;
        this.labelAttributeValue = labelAttributeValue;
        this.dateAttributeName = dateAttributeName;
    }

    public TableKeyStructure(String labelAttributeName, String labelAttributeValue,
            String dateAttributeName, String dateAttributeValue) {
        this.labelAttributeName = labelAttributeName;
        this.labelAttributeValue = labelAttributeValue;
        this.dateAttributeName = dateAttributeName;
        this.dateValues = new HashSet<>();
        this.dateValues.add(dateAttributeValue);
    }

    public TableKeyStructure withDateValue(String dateValue) {
        if (this.dateValues == null) {
            this.dateValues = new HashSet<>();
        }
        this.dateValues.add(dateValue);
        return this;
    }

    public String getLabelAttributeName() {
        return this.labelAttributeName;
    }

    public String getLabelAttributeValue() {
        return this.labelAttributeValue;
    }

    public String getDateAttributeName() {
        return this.dateAttributeName;
    }

    public Set<String> getDateValues() {
        return this.dateValues;
    }
}

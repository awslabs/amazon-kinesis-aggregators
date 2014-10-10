package com.amazonaws.services.kinesis.aggregators.cache;

import com.amazonaws.services.kinesis.aggregators.LabelSet;
import com.amazonaws.services.kinesis.aggregators.TimeHorizon;

/**
 * Class which is used by the object Aggregator as the key to the in-memory
 * version of the AggregateTable.
 */
public class UpdateKey {
    private LabelSet labelValues;

    private String dateAttribute;

    private String dateValue;

    private TimeHorizon timeHorizon;

    public UpdateKey(LabelSet labelValues, String dateAttribute, String dateValue,
            TimeHorizon timeHorizon) {
        this.labelValues = labelValues;
        this.dateAttribute = dateAttribute;
        this.dateValue = dateValue;
        this.timeHorizon = timeHorizon;
    }

    public String getAggregateColumnName() {
        return this.labelValues.getName();
    }

    public String getDateValueColumnName() {
        return this.dateAttribute;
    }

    public String getAggregatedValue() {
        return this.labelValues.valuesAsString();
    }

    public String getDateValue() {
        return this.dateValue;
    }

    public TimeHorizon getTimeHorizon() {
        return this.timeHorizon;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null)
            return false;

        if (!(o instanceof UpdateKey))
            return false;

        UpdateKey other = (UpdateKey) o;
        if (this.labelValues.equals(other.labelValues) && this.dateValue.equals(other.dateValue)) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int res = 17;
        res = 31 * res + (this.labelValues == null ? 0 : this.labelValues.hashCode());
        res = 31 * res + (this.dateValue == null ? 0 : this.dateValue.hashCode());
        return res;
    }

    @Override
    public String toString() {
        return String.format("Update Key - Date Value: %s, Date Column: %s, Label Values: %s",
                this.dateValue, this.dateAttribute, this.labelValues);
    }
}

package com.amazonaws.services.kinesis.aggregators;

import java.util.Date;
import java.util.Map;

public class AggregateData {
    private String uniqueId;

    private LabelSet labels;

    private Date date;

    private Map<String, Double> summaries;

    public AggregateData(String uniqueId, LabelSet labels, Date date, Map<String, Double> summaries) {
        this.uniqueId = uniqueId;
        this.labels = labels;
        this.date = date;
        this.summaries = summaries;
    }

    public String getUniqueId() {
        return this.uniqueId;
    }

    public String getLabel() {
        return this.labels.valuesAsString();
    }

    public LabelSet getLabels() {
        return this.labels;
    }

    public Date getDate() {
        return this.date;
    }

    public Map<String, Double> getSummaries() {
        return this.summaries;
    }
}

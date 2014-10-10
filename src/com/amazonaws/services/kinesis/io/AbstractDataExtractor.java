package com.amazonaws.services.kinesis.io;

import java.util.List;

import com.amazonaws.services.kinesis.aggregators.AggregateData;
import com.amazonaws.services.kinesis.aggregators.AggregatorType;
import com.amazonaws.services.kinesis.aggregators.InputEvent;
import com.amazonaws.services.kinesis.aggregators.exception.SerialisationException;
import com.amazonaws.services.kinesis.aggregators.summary.SummaryConfiguration;

/**
 * Abstract class which provides some helper methods for creating IDataExtractor
 * classes.
 */
public abstract class AbstractDataExtractor implements IDataExtractor {
    protected AggregatorType aggregatorType = AggregatorType.COUNT;

    protected SummaryConfiguration summaryConfig = new SummaryConfiguration();

    public abstract String getAggregateLabelName();

    public abstract String getDateValueName();

    public abstract List<AggregateData> getData(InputEvent event) throws SerialisationException;

    public abstract void validate() throws Exception;

    public AggregatorType getAggregatorType() {
        return this.aggregatorType;
    }

    public void setAggregatorType(AggregatorType type) {
        this.aggregatorType = type;
    }

    public SummaryConfiguration getSummaryConfig() {
        return this.summaryConfig;
    }
}

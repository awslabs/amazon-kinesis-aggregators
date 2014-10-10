package com.amazonaws.services.kinesis.aggregators;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.model.Record;

/**
 * Class which provides a simple automation around a number of aggregators.
 * Register any number of aggregators with the container, and then call all of
 * the registered aggregators aggregate and checkpoint methods through this
 * simple proxy
 */
public class AggregatorGroup implements IStreamAggregator {
    List<StreamAggregator> aggregators = new ArrayList<>();

    public AggregatorGroup() {
    }

    public AggregatorGroup(AggregatorGroup template) throws Exception {
        // create a new aggregator group from all of the aggregators this one
        // encapsulates, by instantiating new aggregators with their copy
        // constructors
        for (StreamAggregator agg : template.aggregators) {
            this.registerAggregator(new StreamAggregator(agg));
        }
    }

    public void registerAggregator(StreamAggregator agg) {
        this.aggregators.add(agg);
    }

    public List<StreamAggregator> getAggregators() {
        return this.aggregators;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void aggregate(List<Record> records) throws Exception {
        for (IStreamAggregator agg : aggregators) {
            agg.aggregate(records);
        }
    }

    public void aggregateEvents(List<InputEvent> events) throws Exception {
        for (IStreamAggregator agg : aggregators) {
            agg.aggregateEvents(events);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void checkpoint() throws Exception {
        for (IStreamAggregator agg : aggregators) {
            agg.checkpoint();
        }
    }

    @Override
    public void initialise(String shardId) throws Exception {
        for (IStreamAggregator agg : aggregators) {
            agg.initialise(shardId);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown(boolean flushState) throws Exception {
        for (IStreamAggregator agg : aggregators) {
            agg.shutdown(flushState);
        }
    }

    /**
     * N/A - use getTableNames()
     */
    @Override
    public String getTableName() {
        return null;
    }

    public List<String> getTableNames() {
        List<String> out = new ArrayList<>(this.aggregators.size());

        for (IStreamAggregator i : this.aggregators) {
            out.add(i.getTableName());
        }

        return out;
    }
}

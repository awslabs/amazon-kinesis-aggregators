package com.amazonaws.services.kinesis.aggregators.summary;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.kinesis.aggregators.exception.UnsupportedCalculationException;

/**
 * The Summary Configuration object contains the required calculations to be
 * performed against summary items extracted from a Kinesis Data Stream. For
 * each item listed as summary value to be extracted from the stream, the
 * Summary configuration will store a list of the calculations against the base
 * item being calculated. For example, if the summary expression was:
 * sum(value_a), max(value_a), min(value_b) then the SummaryConfiguration would
 * be: "key" :[list of
 * {@link com.amazonaws.services.kinesis.aggregators.summary.SummaryCalculation}]
 * --------- ------------ "value_a":[sum,max] "value_b":[min]
 */
public class SummaryConfiguration {
    private Map<String, List<SummaryElement>> config = new HashMap<>();

    /* closure over the map which contains the items to list */
    final class ConfigWriter {
        public void write(String s, SummaryElement e) {
            List<SummaryElement> calculations = config.get(s);

            // setup the list
            if (calculations == null) {
                calculations = new ArrayList<>();
            }

            calculations.add(e);

            config.put(s, calculations);
        }
    }

    private ConfigWriter writer = new ConfigWriter();

    public SummaryConfiguration() {
    }

    public SummaryConfiguration(List<String> summaries) throws UnsupportedCalculationException {
        for (String s : summaries) {
            addConfig(s);
        }
    }

    /**
     * Add a calculation for a base attribute into the list of all calculations
     * to be done
     * 
     * @param value
     * @param calc
     */
    public void add(String value, SummaryElement e) {
        writer.write(value, e);
    }

    private void addConfig(String summary) throws UnsupportedCalculationException {
        SummaryElement e = new SummaryElement(summary);
        add(e.getStreamDataElement(), e);
    }

    /**
     * Add a fully formed expression to the list of all calculations. This uses
     * the parseSummary method to parse the expression into its component parts.
     * 
     * @param summary The expression to add
     * @throws UnsupportedCalculationException
     */
    public SummaryConfiguration withConfigItem(String summary)
            throws UnsupportedCalculationException {
        addConfig(summary);
        return this;
    }

    /**
     * Get all
     * {@link com.amazonaws.services.kinesis.aggregators.summary.SummaryCalculation}s
     * for an attribute item
     * 
     * @param s The attribute of the stream to get the list of calculations for
     * @return
     */
    public List<SummaryElement> getRequestedCalculations(String s) {
        return this.config.get(s);
    }

    /**
     * Get all attributes which this summary configuration is stored against
     * 
     * @return
     */
    public Set<String> getItemSet() {
        return this.config.keySet();
    }
}

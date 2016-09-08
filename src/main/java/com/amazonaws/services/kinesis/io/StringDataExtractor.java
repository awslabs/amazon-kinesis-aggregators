/**
 * Amazon Kinesis Aggregators
 *
 * Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.io;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.aggregators.AggregateData;
import com.amazonaws.services.kinesis.aggregators.AggregatorType;
import com.amazonaws.services.kinesis.aggregators.InputEvent;
import com.amazonaws.services.kinesis.aggregators.LabelSet;
import com.amazonaws.services.kinesis.aggregators.StreamAggregator;
import com.amazonaws.services.kinesis.aggregators.StreamAggregatorUtils;
import com.amazonaws.services.kinesis.aggregators.exception.InvalidConfigurationException;
import com.amazonaws.services.kinesis.aggregators.exception.SerializationException;
import com.amazonaws.services.kinesis.aggregators.exception.UnsupportedCalculationException;
import com.amazonaws.services.kinesis.aggregators.summary.SummaryElement;
import com.amazonaws.services.kinesis.io.serializer.IKinesisSerializer;

/**
 * IDataExtractor implementation which allows for extraction of data from
 * Streams formatted as Character Separated Values. Also optionally allows for
 * regular expression based filtering of the stream prior to aggregation.
 */
public class StringDataExtractor<T extends StringDataExtractor<T>> extends AbstractDataExtractor
        implements IDataExtractor {
    protected List<Integer> labelIndicies = new ArrayList<>();

    private LabelSet labelSet;

    protected String labelAttributeAlias, dateAttributeAlias;

    private boolean usePartitionKeyForUnique = false;

    private boolean useSequenceForUnique = false;

    private int uniqueIdIndex = -1;

    protected int dateValueIndex = -1;

    private String dateFormat;

    private SimpleDateFormat dateFormatter;

    protected List<Object> originalSummaryExpressions = new ArrayList<>();

    protected List<Integer> summaryIndicies = new ArrayList<>();

    protected Map<String, Double> sumUpdates;

    protected IKinesisSerializer<List<List<String>>, byte[]> serialiser;

    private final Log LOG = LogFactory.getLog(StringDataExtractor.class);

    protected StringDataExtractor() {
    }

    /**
     * Validate that the Data Extractor is correctly configured.
     */
    @Override
    public void validate() throws Exception {
        if (this.serialiser == null) {
            throw new InvalidConfigurationException(
                    "Unable to create instance of StringDataExtractor without an IKinesisSerialiser");
        }

        if (aggregatorType.equals(AggregatorType.SUM)
                && (this.summaryIndicies == null || this.summaryIndicies.size() == 0)) {
            throw new InvalidConfigurationException(
                    "Summary type String Aggregators require a list of Summary Indicies");
        }

        this.labelSet = LabelSet.fromIntegerKeys(this.labelIndicies);

        if (this.labelAttributeAlias != null) {
            this.labelSet.withAlias(this.labelAttributeAlias);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AggregateData> getData(InputEvent event) throws SerializationException {
        try {
            int summaryIndex = -1;
            String dateString;
            Date dateValue;
            List<AggregateData> data = new ArrayList<>();

            List<List<String>> content = serialiser.toClass(event);
            for (List<String> line : content) {
                if (line != null) {
                    LabelSet labels = new LabelSet();
                    labels.withAlias(this.labelAttributeAlias);

                    for (Integer key : this.labelIndicies) {
                        labels.put("" + key, line.get(key));
                    }

                    // get the unique index
                    String uniqueId = null;
                    if (this.usePartitionKeyForUnique) {
                        uniqueId = event.getPartitionKey();
                    } else if (this.useSequenceForUnique) {
                        uniqueId = event.getSequenceNumber();
                    } else {
                        if (this.uniqueIdIndex != -1) {
                            uniqueId = line.get(this.uniqueIdIndex);
                        }
                    }

                    // get the date value from the line
                    if (this.dateValueIndex != -1) {
                        dateString = line.get(dateValueIndex);
                        if (this.dateFormat != null) {
                            dateValue = dateFormatter.parse(dateString);
                        } else {
                            // no formatter, so treat as epoch seconds
                            try {
                                dateValue = new Date(Long.parseLong(dateString));
                            } catch (Exception e) {
                                LOG.error(String.format(
                                        "Unable to create Date Value element from item '%s' due to invalid format as Epoch Seconds",
                                        dateValueIndex));
                                throw new SerializationException(e);
                            }
                        }
                    } else {
                        dateValue = new Date(System.currentTimeMillis());
                    }

                    // get the summed values
                    if (this.aggregatorType.equals(AggregatorType.SUM)) {
                        sumUpdates = new HashMap<>();

                        // get the positional sum items
                        for (int i = 0; i < summaryIndicies.size(); i++) {
                            summaryIndex = summaryIndicies.get(i);
                            try {
                                sumUpdates.put("" + summaryIndex,
                                        Double.parseDouble(line.get(summaryIndex)));
                            } catch (NumberFormatException nfe) {
                                LOG.error(String.format(
                                        "Unable to deserialise Summary '%s' due to NumberFormatException",
                                        i));
                                throw new SerializationException(nfe);
                            }
                        }
                    }

                    data.add(new AggregateData(uniqueId, labels, dateValue, sumUpdates));
                }
            }

            return data;
        } catch (Exception e) {
            throw new SerializationException(e);
        }

    }

    /**
     * Builder method to add a date format (based on
     * {@link java.text.SimpleDateFormat} when the dateValueIndex item is a
     * string.
     * 
     * @param dateFormat
     * @return
     */
    @SuppressWarnings("unchecked")
    public T withDateFormat(String dateFormat) {
        if (dateFormat != null && !dateFormat.equals("")) {
            this.dateFormat = dateFormat;
            this.dateFormatter = new SimpleDateFormat(dateFormat);
        }
        return (T) this;
    }

    /**
     * Builder method to add a set of summary indicies or expressions to the
     * aggregation configuration.
     * 
     * @param summaryIndicies List of integer values indicating positions in the
     *        stream for summary values, or a list of strings indicating
     *        expressions around positions which contain summary values to be
     *        aggregated. If expressions using
     *        {@link com.amazonaws.services.kinesis.aggregators.summary.SummaryCalculation}
     *        are used, then the format is SummaryCalculation(index), for
     *        example the sum of position 4 would be 'sum(4)'
     * @return
     * @throws UnsupportedCalculationException
     */
    @SuppressWarnings("unchecked")
    public T withSummaryIndicies(List<Object> summaryIndicies)
            throws UnsupportedCalculationException {
        if (summaryIndicies != null) {
            for (Object o : summaryIndicies) {
                if (o instanceof Integer) {
                    Integer i = (Integer) o;
                    withSummaryIndex(i);
                } else if (o instanceof String) {
                    String s = (String) o;
                    withSummaryIndex(s);
                } else {
                    throw new UnsupportedCalculationException(String.format(
                            "Unable to generate calculation for %s Datatype",
                            o.getClass().getSimpleName()));
                }
            }
        }

        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withStringSummaryIndicies(List<String> summaryIndicies)
            throws UnsupportedCalculationException {
        if (summaryIndicies != null) {
            for (String s : summaryIndicies) {
                withSummaryIndex(s);
            }
        }

        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withIntegerSummaryIndicies(List<Integer> summaryIndicies)
            throws UnsupportedCalculationException {
        if (summaryIndicies != null) {
            for (Integer i : summaryIndicies) {
                withSummaryIndex(i);
            }
        }

        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withSummaryIndex(Integer index) {
        this.aggregatorType = AggregatorType.SUM;

        this.summaryIndicies.add(index);
        this.originalSummaryExpressions.add(index);
        try {
            this.summaryConfig.withConfigItem(String.format("sum(%s)", index));
        } catch (UnsupportedCalculationException e) {
        }

        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withSummaryIndex(String expression) throws UnsupportedCalculationException {
        this.aggregatorType = AggregatorType.SUM;

        if (this.summaryIndicies == null) {
            this.summaryIndicies = new ArrayList<>();
        }

        SummaryElement e = new SummaryElement(expression);

        this.originalSummaryExpressions.add(expression);
        this.summaryIndicies.add(Integer.parseInt(e.getStreamDataElement()));
        this.summaryConfig.withConfigItem(expression);

        return (T) this;
    }

    public T withLabelAttributeAlias(String alias) {
        this.labelAttributeAlias = alias;

        return (T) this;
    }

    public T withUniqueIdIndex(String index) {
        switch (index) {
            case StreamAggregator.REF_PARTITION_KEY:
                this.usePartitionKeyForUnique = true;
                break;
            case StreamAggregator.REF_SEQUENCE:
                this.useSequenceForUnique = true;
                break;
            default:
                this.uniqueIdIndex = Integer.parseInt(index);

                break;
        }

        return (T) this;
    }

    public T withDateAttributeAlias(String alias) {
        this.dateAttributeAlias = alias;

        return (T) this;
    }

    /**
     * {@inheritDoc}
     */
    public String getAggregateLabelName() {
        return this.labelSet.getName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getUniqueIdName() {
        return "" + this.uniqueIdIndex;
    }

    /**
     * {@inheritDoc}
     */
    public String getDateValueName() {
        return this.dateAttributeAlias != null ? this.dateAttributeAlias : "" + this.dateValueIndex;
    }

    public List<Object> getOriginalSummaryExpressions() {
        return this.originalSummaryExpressions;
    }

    public IDataExtractor copy() throws Exception {
        throw new UnsupportedOperationException();
    }
}

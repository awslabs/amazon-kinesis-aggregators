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
package com.amazonaws.services.kinesis.aggregators.cache;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.kinesis.aggregators.StreamAggregator;
import com.amazonaws.services.kinesis.aggregators.datastore.AggregateAttributeModification;
import com.amazonaws.services.kinesis.aggregators.summary.SummaryCalculation;
import com.amazonaws.services.kinesis.aggregators.summary.SummaryElement;

/**
 * Class which is used as the payload container for data which is cached in the
 * Aggregator prior to checkpointing.
 */
public class UpdateValue {
    private double aggregateCount;

    /*
     * The pending update summaries are comprised of the value to be applied to
     * the attribute, the calculation that was applied to get that value, and
     * the original value from the stream used to extract the data
     */
    private Map<String, AggregateAttributeModification> summaryValues;

    private String lastWriteSeq;

    private long lastWriteTime;

    public UpdateValue() {
        this.aggregateCount = 0;
        this.summaryValues = new HashMap<>();
    }

    public void incrementCount(int count) {
        this.aggregateCount += count;
    }

    public void updateSummary(String label, double withValue, SummaryElement element) {
        // apply the calculation to the old and new values in the update
        // payload
        AggregateAttributeModification current = this.summaryValues.get(element.getAttributeAlias());
        Double currentValue = current == null ? null : current.getFinalValue();

        // apply the calculation using the apply method
        Double newValue = element.getCalculation().apply(currentValue, withValue);

        // build the summary value to be tracked in memory
        AggregateAttributeModification update = new AggregateAttributeModification(
                element.getAttributeAlias(), label, currentValue, withValue, newValue,
                element.getCalculation(), 0);

        // update the in memory version of the update payload for the label
        this.summaryValues.put(element.getAttributeAlias(), update);
    }

    public void lastWrite(String lastSeq, long lastTime) {
        this.lastWriteSeq = lastSeq;
        this.lastWriteTime = lastTime;
    }

    public double getAggregateCount() {
        return aggregateCount;
    }

    public double getSummaryValue(String label) {
        return getSummary(label).getFinalValue();
    }

    public AggregateAttributeModification getSummary(String label) {
        return this.summaryValues.get(label);
    }

    public AggregateAttributeModification getValueByOriginal(String attributeName,
            SummaryCalculation calculation) {
        return this.summaryValues.get(SummaryElement.makeStoreAttributeName(attributeName,
                calculation));
    }

    public Map<String, AggregateAttributeModification> getSummaryValues() {
        return this.summaryValues;
    }

    public String getLastWriteSeq() {
        return lastWriteSeq;
    }

    public long getLastWriteTime() {
        return lastWriteTime;
    }

    @Override
    public String toString() {
        String summary = "";
        if (this.summaryValues != null && this.summaryValues.size() > 0) {
            summary = ",";
            for (String s : this.summaryValues.keySet()) {
                summary = summary + summaryValues.get(s).toString() + ",";
            }
            summary = summary.substring(0, summary.length() - 1);
        }
        return String.format(
                "Update Value - Aggregate Count: %s, Last Write Seq: %s, Last Write Time: %s%s",
                this.aggregateCount, this.lastWriteSeq,
                StreamAggregator.dateFormatter.format(this.lastWriteTime), summary);
    }
}

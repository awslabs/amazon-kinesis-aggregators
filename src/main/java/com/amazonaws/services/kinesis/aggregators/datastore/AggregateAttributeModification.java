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
package com.amazonaws.services.kinesis.aggregators.datastore;

import com.amazonaws.services.kinesis.aggregators.summary.SummaryCalculation;

public class AggregateAttributeModification {
    private String attributeName, originatingValueName;

    private Double oldValue, newValue, finalValue;

    private SummaryCalculation calculationApplied;

    private int writesSoFar;

    private AggregateAttributeModification() {
    }

    public AggregateAttributeModification(String attributeName, String originatingValueName,
            Double finalValue, SummaryCalculation calculationApplied) {
        this(attributeName, originatingValueName, null, null, finalValue, calculationApplied, 0);
    }

    public AggregateAttributeModification(String attributeName, String originatingValueName,
            Double finalValue, SummaryCalculation calculationApplied, int writesSoFar) {
        this(attributeName, originatingValueName, null, null, finalValue, calculationApplied,
                writesSoFar);
    }

    public AggregateAttributeModification(String attributeName, String originatingValueName,
            Double oldValue, Double newValue, Double finalValue,
            SummaryCalculation calculationApplied, int writesSoFar) {
        this.attributeName = attributeName;
        this.originatingValueName = originatingValueName;
        this.oldValue = oldValue;
        this.newValue = newValue;
        this.finalValue = finalValue;
        this.calculationApplied = calculationApplied;
        this.writesSoFar = writesSoFar;
    }

    public String getAttributeName() {
        return attributeName;
    }

    public String getOriginatingValueName() {
        return originatingValueName;
    }

    public Double getOldValue() {
        return oldValue;
    }

    public Double getNewValue() {
        return newValue;
    }

    public Double getFinalValue() {
        return finalValue;
    }

    public SummaryCalculation getCalculationApplied() {
        return calculationApplied;
    }

    public int getWritesSoFar() {
        return writesSoFar;
    }

    @Override
    public String toString() {
        return String.format(
                "Aggregate Attribute Modification - Originating Value Name: %s, Attribute Name: %s, Calculation Applied: %s, Old Value: %s, New Value: %s, Final Value: %s",
                this.originatingValueName, this.attributeName, this.calculationApplied.name(),
                this.oldValue, this.newValue, this.finalValue);
    }
}

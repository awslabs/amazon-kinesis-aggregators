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
package com.amazonaws.services.kinesis.aggregators.summary;

import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.kinesis.aggregators.datastore.DynamoDataStore.DynamoSummaryUpdateMethod;

public enum SummaryCalculation {
    /**
     * SUM Calculations simply always increase the aggregate value based upon
     * the value observed on the stream
     */
    SUM(null, DynamoSummaryUpdateMethod.ADD) {
        @Override
        public Double apply(Double currentValue, Double newValue) {
            // add the values including dealing with nulls
            return nvl(currentValue) + nvl(newValue);
        }
    },

    /**
     * FIRST Calculations always return the first value observed, without
     * considering the newest
     */
    FIRST(null, DynamoSummaryUpdateMethod.CONDITIONAL) {
        @Override
        public Double apply(Double currentValue, Double newValue) {
            // always return the current value unless its null, then overwrite
            // with the first value
            return currentValue == null ? newValue : currentValue;
        }
    },

    /**
     * LAST Calculations always return the latest value observed, without
     * considering the previous
     */
    LAST(null, DynamoSummaryUpdateMethod.PUT) {
        @Override
        public Double apply(Double currentValue, Double newValue) {
            // always return the latest value
            return nvl(newValue);
        }
    },

    /**
     * The Min calculation seeks to always record only the lowest value ever
     * observed for a data value in the specified time horizon
     */
    MIN(ComparisonOperator.GT, DynamoSummaryUpdateMethod.CONDITIONAL) {
        // The comparison operator is compared to the existing values. So to
        // apply a
        // minimum value, the existing value should be greater than the new
        // value
        @Override
        public Double apply(Double currentValue, Double newValue) {
            // the lower value wins, or 0 if values have not yet been
            // initialised
            if (currentValue == null)
                return nvl(newValue);

            if (newValue == null)
                return nvl(currentValue);

            Double output = nvl(newValue) < currentValue ? nvl(newValue) : currentValue;

            return output;
        }
    },

    /**
     * The Max calculation will store only the maximum value observed on the
     * stream for the time period
     */
    MAX(ComparisonOperator.LT, DynamoSummaryUpdateMethod.CONDITIONAL) {
        // apply a new value only if the existing value is less than the new
        // value
        @Override
        public Double apply(Double currentValue, Double newValue) {
            // the greater value wins, or 0 if values have not yet been
            // initialised
            if (currentValue == null)
                return nvl(newValue);

            if (newValue == null)
                return nvl(currentValue);

            return nvl(newValue) > currentValue ? nvl(newValue) : currentValue;
        }
    };

    private ComparisonOperator comparisonOperator;

    private DynamoSummaryUpdateMethod updateMethod;

    private SummaryCalculation(ComparisonOperator c, DynamoSummaryUpdateMethod updateMethod) {
        this.comparisonOperator = c;
        this.updateMethod = updateMethod;
    }

    private SummaryCalculation() {
    }

    /**
     * Apply the calculation to the values provided to the interface
     * 
     * @param currentValue The current aggregate value being managed by the
     *        {@link com.amazonaws.services.kinesis.aggregators.cache.AggregateCache}
     * @param newValue The new value from the stream to be applied to the
     *        calculation
     * @return
     */
    public abstract Double apply(Double currentValue, Double newValue);

    /**
     * Return the
     * {@link com.amazonaws.services.dynamodbv2.model.ComparisonOperator} which
     * will be applied when this calculation is written to the database
     * 
     * @return
     */
    public ComparisonOperator getDynamoComparisonOperator() {
        return this.comparisonOperator;
    }

    public DynamoSummaryUpdateMethod getSummaryUpdateMethod() {
        return this.updateMethod;
    }

    private static double nvl(Double val) {
        return val == null ? 0D : val;
    }
}

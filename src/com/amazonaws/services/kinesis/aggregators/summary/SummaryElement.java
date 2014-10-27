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

import com.amazonaws.services.kinesis.aggregators.StreamAggregatorUtils;
import com.amazonaws.services.kinesis.aggregators.exception.UnsupportedCalculationException;

public class SummaryElement {
    private String streamDataElement, attributeAlias;

    private SummaryCalculation calculation;

    public SummaryElement(String streamDataElement, SummaryCalculation calculation) {
        this(streamDataElement, calculation, makeStoreAttributeName(streamDataElement, calculation));

    }

    public SummaryElement(String streamDataElement, SummaryCalculation calculation,
            String attributeAlias) {
        this.streamDataElement = streamDataElement;
        this.calculation = calculation;
        if (attributeAlias != null) {
            this.attributeAlias = attributeAlias;
        } else {
            this.attributeAlias = makeStoreAttributeName(streamDataElement, calculation);
        }
    }

    /**
     * Parse a summary calculation expression to a Pair of the base item name,
     * and the SummaryCalculation to be applied to that base item. The
     * expression must take the form of:
     * {@link com.amazonaws.services.kinesis.aggregators.summary.SummaryCalculation}
     * (attribute of the data stream)
     * 
     * @param s
     * @return
     * @throws UnsupportedCalculationException
     */
    public SummaryElement(String s) throws UnsupportedCalculationException {
        if (!s.contains("(")) {
            this.streamDataElement = s;
            this.calculation = SummaryCalculation.SUM;
            this.attributeAlias = makeStoreAttributeName(s, this.calculation);
        } else {
            if (!s.contains(")"))
                throw new UnsupportedCalculationException(String.format(
                        "\"%s\" is not a valid summary calculation", s));

            String[] tokens = s.split("\\(");
            String requested = tokens[0].replaceAll(" ", "").toUpperCase();

            try {
                SummaryCalculation c = SummaryCalculation.valueOf(requested);
                String[] onItems = tokens[1].split("\\)");
                this.streamDataElement = onItems[0].replaceAll(" ", "");
                this.calculation = c;
                if (onItems.length > 1 && onItems[1] != null) {
                    this.attributeAlias = onItems[1].replaceAll(" ", "");
                } else {
                    this.attributeAlias = makeStoreAttributeName(this.streamDataElement,
                            this.calculation);
                }
            } catch (Exception e) {
                throw new UnsupportedCalculationException(String.format(
                        "Unsupported Calculation %s", requested));
            }
        }
    }

    public static String makeStoreAttributeName(String attribute, SummaryCalculation calculation) {
        return String.format("%s-%s", StreamAggregatorUtils.methodToColumn(attribute),
                calculation.name());
    }

    public String getStreamDataElement() {
        return streamDataElement;
    }

    public String getAttributeAlias() {
        return attributeAlias;
    }

    public SummaryCalculation getCalculation() {
        return calculation;
    }
}

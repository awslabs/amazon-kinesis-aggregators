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
package com.amazonaws.services.kinesis.aggregators;

import java.util.HashSet;
import java.util.Set;

public class TableKeyStructure {
    private String labelAttributeName, labelAttributeValue, dateAttributeName;

    private Set<String> dateValues;

    public TableKeyStructure() {
    }

    public TableKeyStructure(String labelAttributeName, String labelAttributeValue,
            String dateAttributeName) {
        this.labelAttributeName = labelAttributeName;
        this.labelAttributeValue = labelAttributeValue;
        this.dateAttributeName = dateAttributeName;
    }

    public TableKeyStructure(String labelAttributeName, String labelAttributeValue,
            String dateAttributeName, String dateAttributeValue) {
        this.labelAttributeName = labelAttributeName;
        this.labelAttributeValue = labelAttributeValue;
        this.dateAttributeName = dateAttributeName;
        this.dateValues = new HashSet<>();
        this.dateValues.add(dateAttributeValue);
    }

    public TableKeyStructure withDateValue(String dateValue) {
        if (this.dateValues == null) {
            this.dateValues = new HashSet<>();
        }
        this.dateValues.add(dateValue);
        return this;
    }

    public String getLabelAttributeName() {
        return this.labelAttributeName;
    }

    public String getLabelAttributeValue() {
        return this.labelAttributeValue;
    }

    public String getDateAttributeName() {
        return this.dateAttributeName;
    }

    public Set<String> getDateValues() {
        return this.dateValues;
    }
}

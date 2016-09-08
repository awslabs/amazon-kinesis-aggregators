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
package com.amazonaws.services.kinesis.aggregators.exception;

import com.amazonaws.services.kinesis.aggregators.summary.SummaryCalculation;

/**
 * Exception thrown when a summary value is indicated that is not one of
 * {@link com.amazonaws.services.kinesis.aggregators.summary.SummaryCalculation}
 */
public class UnsupportedCalculationException extends Exception {
    private String message;

    public UnsupportedCalculationException(String message) {
        super();
        this.message = message;
    }

    @Override
    public String getMessage() {
        return this.message;
    }
}

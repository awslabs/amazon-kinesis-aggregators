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
package com.amazonaws.services.kinesis.aggregators.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;

import com.amazonaws.services.kinesis.aggregators.summary.SummaryCalculation;

/**
 * Annotation which indicates that a method should be used as a summary
 * aggregation. If no type is indicated then it will be used as a
 * {@link com.amazonaws.services.kinesis.aggregators.SummaryCalculation.SUM}.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Summary {
    /**
     * The type of summary calculations to apply to the method.
     * 
     * @return
     */
    public SummaryCalculation[] type() default SummaryCalculation.SUM;
}

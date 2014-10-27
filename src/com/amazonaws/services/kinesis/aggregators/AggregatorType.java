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

/**
 * Types of Aggregators supported by the Kinesis Aggregator Framework.
 */
public enum AggregatorType {
    /**
     * Count Aggregators maintain only an Event Count observed for the indicated
     * {@link com.amazonaws.services.kinesis.aggregators.TimeHorizon}
     */
    COUNT,
    /**
     * Sum Aggregators maintain an Event Count, plus a set of summary values for
     * data indicated on the stream as being a summary value. Summary Values can
     * be any of
     * {@link com.amazonaws.services.kinesis.aggregators.SummaryCalculation}
     */
    SUM;
}

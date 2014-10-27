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

import com.amazonaws.services.kinesis.aggregators.AggregatorType;
import com.amazonaws.services.kinesis.aggregators.TimeHorizon;
import com.amazonaws.services.kinesis.aggregators.datastore.DynamoDataStore;
import com.amazonaws.services.kinesis.aggregators.metrics.CloudWatchMetricsEmitter;

/**
 * Annotations to indicate that a Class contains an Aggregator Configuration
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Aggregate {
    /**
     * The type of Aggregator to create. Default is COUNT.
     * 
     * @return
     */
    AggregatorType type() default AggregatorType.COUNT;

    /** The list of Time Horizons to Aggregate on */
    TimeHorizon[] timeHorizons() default TimeHorizon.HOUR;

    int[] timeGranularity() default -1;

    /**
     * The namespace for the Aggregation Data.
     * 
     * @return
     */
    String namespace() default "";

    /**
     * Should the Aggregator fail on errors in reading data from the stream for
     * Aggregation.
     * 
     * @return
     */
    boolean failOnDataExtractionErrors() default true;

    /**
     * Should the aggregator publish intrumentation metrics? The default metrics
     * emitter is CloudWatch
     * 
     * @return
     */
    boolean emitMetrics() default false;

    /**
     * Configure an IDataStore other than the default Dynamo DB Datastore
     * 
     * @return
     */
    Class dataStore() default DynamoDataStore.class;

    /**
     * Configure an IMetricsEmitter other than the default CloudWatch metrics
     * service
     */
    Class metricsEmitter() default CloudWatchMetricsEmitter.class;
}

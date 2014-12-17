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
package com.amazonaws.services.kinesis.aggregators.metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClient;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.LimitExceededException;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.kinesis.aggregators.StreamAggregator;
import com.amazonaws.services.kinesis.aggregators.cache.UpdateKey;
import com.amazonaws.services.kinesis.aggregators.datastore.AggregateAttributeModification;

public class CloudWatchMetricsEmitter implements IMetricsEmitter {
    private final Log LOG = LogFactory.getLog(CloudWatchMetricsEmitter.class);

    private String metricsNamespace;

    private AmazonCloudWatchClient cloudWatchClient;

    private Region region;

    private static final int THROTTLING_RETRIES = 10;

    private static final int BACKOFF_MILLIS = 10;

    private static final int MAX_WRITE_ATTEMPTS = 10;

    public CloudWatchMetricsEmitter() {
    }

    public CloudWatchMetricsEmitter(String metricsNamespace, AWSCredentialsProvider credentials) {
        this.metricsNamespace = metricsNamespace;
        this.cloudWatchClient = new AmazonCloudWatchAsyncClient(credentials);
    }

    @Override
    public void emit(Map<UpdateKey, Map<String, AggregateAttributeModification>> metricData)
            throws Exception {
        if (metricData != null) {
            Date metricDate = null;

            for (UpdateKey key : metricData.keySet()) {
                PutMetricDataRequest req = new PutMetricDataRequest().withNamespace(this.metricsNamespace);
                Collection<MetricDatum> data = new ArrayList<>();

                metricDate = StreamAggregator.dateFormatter.parse(key.getDateValue());

                // send in every update as a datum
                for (String summary : metricData.get(key).keySet()) {
                    final AggregateAttributeModification mod = metricData.get(key).get(summary);
                    // TODO Handle that we've been sent an update for which a
                    // new final value which might not have been set. This
                    // means, for example, that on an hourly aggregate of FIRST,
                    // we'd get a single modification at the beginning of the
                    // hour, and then not again after
                    if (mod.getFinalValue() != null) {
                        data.add(new MetricDatum().withMetricName(mod.getOriginatingValueName()).withTimestamp(
                                metricDate).withDimensions(
                                new Dimension().withName("Calculation").withValue(
                                        mod.getCalculationApplied().name()),
                                new Dimension().withName(key.getAggregateColumnName()).withValue(
                                        key.getAggregatedValue())).withValue(mod.getFinalValue()));
                    }
                }

                boolean success = false;
                int iterations = 0;
                int backoffMillis = BACKOFF_MILLIS;
                while (!success && iterations < MAX_WRITE_ATTEMPTS) {
                    iterations++;
                    boolean backoff = false;
                    try {
                        cloudWatchClient.putMetricData(req.withMetricData(data));
                        success = true;
                    } catch (LimitExceededException e) {
                        backoff = true;
                    } catch (AmazonServiceException ase) {
                        if (ase.getErrorCode().startsWith("Throttling")) {
                            backoff = true;
                        }
                    }

                    if (backoff) {
                        LOG.warn("CloudWatch Limit Exceeded - backing off");
                        Thread.sleep(2 ^ iterations * BACKOFF_MILLIS);
                    }
                }

                if (!success) {
                    throw new MetricsEmitterThrottledException(String.format(
                            "CloudWatch Metrics Emitter failed to write metrics after %s attempts",
                            MAX_WRITE_ATTEMPTS));
                }
            }
        }
    }

    @Override
    public void setRegion(Region region) {
        this.region = region;
        this.cloudWatchClient.setRegion(region);
    }
}

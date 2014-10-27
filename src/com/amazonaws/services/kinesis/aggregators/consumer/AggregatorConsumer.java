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
package com.amazonaws.services.kinesis.aggregators.consumer;

import java.net.NetworkInterface;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.aggregators.AggregatorGroup;
import com.amazonaws.services.kinesis.aggregators.AggregatorsConstants;
import com.amazonaws.services.kinesis.aggregators.StreamAggregator;
import com.amazonaws.services.kinesis.aggregators.exception.InvalidConfigurationException;
import com.amazonaws.services.kinesis.aggregators.factory.ExternallyConfiguredAggregatorFactory;
import com.amazonaws.services.kinesis.aggregators.processor.AggregatorProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

public final class AggregatorConsumer {

    private static final Log LOG = LogFactory.getLog(AggregatorConsumer.class);

    private String streamName, appName, regionName, environmentName, configFilePath,
            positionInStream, kinesisEndpoint;

    private AWSCredentialsProvider credentialsProvider;

    private boolean emitMetrics = false;

    private InitialPositionInStream streamPosition;

    private int failuresToTolerate = -1;

    private int maxRecords = -1;

    private KinesisClientLibConfiguration config;

    private AggregatorGroup aggGroup;

    private boolean isConfigured = false;

    public AggregatorConsumer(String streamName, String appName, String configFilePath) {
        this.streamName = streamName;
        this.appName = appName;
        this.configFilePath = configFilePath;
    }

    private AggregatorGroup buildAggregatorsFromConfig() throws Exception {
        return ExternallyConfiguredAggregatorFactory.buildFromConfig(this.streamName, this.appName,
                this.config, configFilePath);

    }

    public void shutdown() throws Exception {
        this.aggGroup.shutdown(true);
    }

    public int run() throws Exception {
        configure();

        System.out.println(String.format("Starting %s", appName));
        LOG.info(String.format("Running %s to process stream %s", appName, streamName));

        IRecordProcessorFactory recordProcessorFactory = new AggregatorProcessorFactory(aggGroup);
        Worker worker = new Worker(recordProcessorFactory, this.config);

        int exitCode = 0;
        int failures = 0;

        // run the worker, tolerating as many failures as is configured
        while (failures < failuresToTolerate || failuresToTolerate == -1) {
            try {
                worker.run();
            } catch (Throwable t) {
                LOG.error("Caught throwable while processing data.", t);

                failures++;

                if (failures < failuresToTolerate) {
                    LOG.error("Restarting...");
                }
                exitCode = 1;
            }
        }

        return exitCode;
    }

    private void assertThat(boolean condition, String message) throws Exception {
        if (!condition) {
            throw new InvalidConfigurationException(message);
        }
    }

    private void validateConfig() throws InvalidConfigurationException {
        try {
            assertThat(this.streamName != null, "Must Specify a Stream Name");
            assertThat(this.appName != null, "Must Specify an Application Name");
        } catch (Exception e) {
            throw new InvalidConfigurationException(e.getMessage());
        }
    }

    public void configure() throws Exception {
        if (!isConfigured) {
            validateConfig();

            if (this.positionInStream != null) {
                streamPosition = InitialPositionInStream.valueOf(this.positionInStream);
            } else {
                streamPosition = InitialPositionInStream.LATEST;
            }

            // append the environment name to the application name
            if (environmentName != null) {
                appName = String.format("%s-%s", appName, environmentName);
            }

            // ensure the JVM will refresh the cached IP values of AWS resources
            // (e.g. service endpoints).
            java.security.Security.setProperty("networkaddress.cache.ttl", "60");

            String workerId = NetworkInterface.getNetworkInterfaces() + ":" + UUID.randomUUID();
            LOG.info("Using Worker ID: " + workerId);

            // obtain credentials using the default provider chain or the
            // credentials provider supplied
            AWSCredentialsProvider credentialsProvider = this.credentialsProvider == null ? new DefaultAWSCredentialsProviderChain()
                    : this.credentialsProvider;

            LOG.info("Using credentials with Access Key ID: "
                    + credentialsProvider.getCredentials().getAWSAccessKeyId());

            config = new KinesisClientLibConfiguration(appName, streamName, credentialsProvider,
                    workerId).withInitialPositionInStream(streamPosition).withKinesisEndpoint(
                    kinesisEndpoint);

            config.getKinesisClientConfiguration().setUserAgent(StreamAggregator.AWSApplication);

            if (regionName != null) {
                Region region = Region.getRegion(Regions.fromName(regionName));
                config.withRegionName(region.getName());
            }

            if (maxRecords != -1)
                config.withMaxRecords(maxRecords);

            // initialise the Aggregators
            aggGroup = buildAggregatorsFromConfig();

            LOG.info(String.format(
                    "Amazon Kinesis Aggregators Managed Client prepared for %s on %s in %s (%s) using %s Max Records",
                    config.getApplicationName(), config.getStreamName(), config.getRegionName(),
                    config.getWorkerIdentifier(), config.getMaxRecords()));

            isConfigured = true;
        }
    }

    public AggregatorConsumer withKinesisEndpoint(String kinesisEndpoint) {
        this.kinesisEndpoint = kinesisEndpoint;
        return this;
    }

    public AggregatorConsumer withToleratedWorkerFailures(int failuresToTolerate) {
        this.failuresToTolerate = failuresToTolerate;
        return this;
    }

    public AggregatorConsumer withMaxRecords(int maxRecords) {
        this.maxRecords = maxRecords;
        return this;
    }

    public AggregatorConsumer withRegionName(String regionName) {
        this.regionName = regionName;
        return this;
    }

    public AggregatorConsumer withEnvironment(String environmentName) {
        this.environmentName = environmentName;
        return this;
    }

    public AggregatorConsumer withCredentialsProvider(AWSCredentialsProvider credentialsProvider) {
        this.credentialsProvider = credentialsProvider;
        return this;
    }

    public AggregatorConsumer withInitialPositionInStream(String positionInStream) {
        this.positionInStream = positionInStream;
        return this;
    }

    public AggregatorConsumer withMetricsEmitter() {
        this.emitMetrics = true;
        return this;
    }

    public AggregatorGroup getAggregators() {
        return this.aggGroup;
    }

    public static void main(String[] args) throws Exception {
        String streamName = System.getProperty(AggregatorsConstants.STREAM_NAME_PARAM);
        String appName = System.getProperty(AggregatorsConstants.APP_NAME_PARAM);
        String configFilePath = System.getProperty(AggregatorsConstants.CONFIG_PATH_PARAM);
        String regionName = System.getProperty(AggregatorsConstants.REGION_PARAM);
        String failuresToTolerate = System.getProperty(AggregatorsConstants.FAILURES_TOLERATED_PARAM);
        String maxRecords = System.getProperty(AggregatorsConstants.MAX_RECORDS_PARAM);
        String environmentName = System.getProperty(AggregatorsConstants.ENVIRONMENT_PARAM);
        String positionInStream = System.getProperty(AggregatorsConstants.STREAM_POSITION_PARAM);

        AggregatorConsumer consumer = new AggregatorConsumer(streamName, appName, configFilePath);

        // add optional configuration items
        if (regionName != null && regionName != "") {
            consumer.withRegionName(regionName);
        }

        if (failuresToTolerate != null && failuresToTolerate != "") {
            consumer.withToleratedWorkerFailures(Integer.parseInt(failuresToTolerate));
        }

        if (maxRecords != null && maxRecords != "") {
            consumer.withMaxRecords(Integer.parseInt(maxRecords));
        }

        if (environmentName != null && environmentName != "") {
            consumer.withEnvironment(environmentName);
        }

        if (positionInStream != null && positionInStream != "") {
            consumer.withInitialPositionInStream(positionInStream);
        }

        System.exit(consumer.run());
    }
}

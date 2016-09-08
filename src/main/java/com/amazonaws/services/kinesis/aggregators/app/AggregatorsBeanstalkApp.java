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
package com.amazonaws.services.kinesis.aggregators.app;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.aggregators.AggregatorGroup;
import com.amazonaws.services.kinesis.aggregators.AggregatorsConstants;
import com.amazonaws.services.kinesis.aggregators.consumer.AggregatorConsumer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;

public class AggregatorsBeanstalkApp implements ServletContextListener {
    private static final Log LOG = LogFactory.getLog(AggregatorsBeanstalkApp.class);

    protected static final String AGGREGATOR_GROUP_PARAM = "aggregator-group";

    private AggregatorConsumer consumer;

    private Thread t;

    @Override
    public void contextDestroyed(ServletContextEvent arg0) {
        try {
            consumer.shutdown();
            t.interrupt();
        } catch (Exception e) {
            LOG.error(e);
        }
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public void contextInitialized(ServletContextEvent contextEvent) {
        String configPath = System.getProperty(AggregatorsConstants.CONFIG_URL_PARAM);

        if (configPath != null && !configPath.equals("")) {
            LOG.info("Starting Managed Beanstalk Aggregators Worker");
            String streamNameParam = System.getProperty(AggregatorsConstants.STREAM_NAME_PARAM);
            String appNameParam = System.getProperty(AggregatorsConstants.APP_NAME_PARAM);
            String regionNameParam = System.getProperty(AggregatorsConstants.REGION_PARAM);
            String streamPosParam = System.getProperty(AggregatorsConstants.STREAM_POSITION_PARAM);
            String maxRecordsParam = System.getProperty(AggregatorsConstants.MAX_RECORDS_PARAM);
            String environmentParam = System.getProperty(AggregatorsConstants.ENVIRONMENT_PARAM);
            String failuresToleratedParam = System.getProperty(AggregatorsConstants.FAILURES_TOLERATED_PARAM);

            if (streamNameParam == null || streamNameParam.equals("") || appNameParam == null
                    || appNameParam.equals("")) {
                LOG.error(String.format(
                        "Unable to run Beanstalk Managed Aggregator Consumer without Configuration of Parameters %s and %s. Application is Idle.",
                        AggregatorsConstants.STREAM_NAME_PARAM, AggregatorsConstants.APP_NAME_PARAM));
                return;
            }

            InitialPositionInStream initialPosition = null;
            if (streamPosParam != null) {
                try {
                    initialPosition = InitialPositionInStream.valueOf(streamPosParam);
                    LOG.info(String.format("Starting from %s Position in Stream", streamPosParam));
                } catch (Exception e) {
                    LOG.error(String.format("%s is an invalid Initial Position in Stream",
                            streamPosParam));
                    return;
                }
            }

            try {
                AggregatorConsumer consumer = new AggregatorConsumer(streamNameParam, appNameParam,
                        configPath);

                // add consumer parameters, if set from System Properties
                if (regionNameParam != null && !regionNameParam.equals("")) {
                    consumer.withRegionName(regionNameParam);
                }

                if (initialPosition != null) {
                    consumer.withInitialPositionInStream(initialPosition.name());
                }

                if (maxRecordsParam != null && !maxRecordsParam.equals("")) {
                    consumer.withMaxRecords(Integer.parseInt(maxRecordsParam));
                }

                if (environmentParam != null && !environmentParam.equals("")) {
                    consumer.withEnvironment(environmentParam);
                }

                if (failuresToleratedParam != null && !failuresToleratedParam.equals("")) {
                    consumer.withToleratedWorkerFailures(Integer.parseInt(failuresToleratedParam));
                }

                // configure the consumer so that the aggregators get
                // instantiated
                consumer.configure();

                AggregatorGroup aggGroup = consumer.getAggregators();

                // put the aggregator group reference and configureation
                // references into the application context
                contextEvent.getServletContext().setAttribute(AGGREGATOR_GROUP_PARAM, aggGroup);
                contextEvent.getServletContext().setAttribute(
                        AggregatorsConstants.STREAM_NAME_PARAM, streamNameParam);

                LOG.info("Registered Stream and Aggregator Group with Servlet Context");

                // start the consumer
                final class ConsumerRunner implements Runnable {
                    final AggregatorConsumer consumer;

                    public ConsumerRunner(AggregatorConsumer consumer) {
                        this.consumer = consumer;
                    }

                    @Override
                    public void run() {
                        try {
                            consumer.run();
                        } catch (Exception e) {
                            e.printStackTrace();
                            LOG.error(e);
                        }
                    }
                }
                t = new Thread(new ConsumerRunner(consumer));
                t.start();
            } catch (Exception e) {
                LOG.error(e);
            }
        } else {
            LOG.warn(String.format(
                    "No Aggregators Configuration File found in Beanstalk Configuration %s. Application is Idle",
                    AggregatorsConstants.CONFIG_URL_PARAM));
        }
    }
}

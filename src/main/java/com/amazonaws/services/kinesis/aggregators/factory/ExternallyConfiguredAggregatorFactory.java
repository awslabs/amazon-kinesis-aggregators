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
package com.amazonaws.services.kinesis.aggregators.factory;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.kinesis.aggregators.AggregatorGroup;
import com.amazonaws.services.kinesis.aggregators.StreamAggregator;
import com.amazonaws.services.kinesis.aggregators.configuration.ExternalConfigurationModel;
import com.amazonaws.services.kinesis.aggregators.datastore.IDataStore;
import com.amazonaws.services.kinesis.aggregators.exception.InvalidConfigurationException;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.io.CsvDataExtractor;
import com.amazonaws.services.kinesis.io.IDataExtractor;
import com.amazonaws.services.kinesis.io.JsonDataExtractor;
import com.amazonaws.services.kinesis.io.ObjectExtractor;
import com.amazonaws.services.kinesis.io.RegexDataExtractor;

public class ExternallyConfiguredAggregatorFactory {
    private ExternallyConfiguredAggregatorFactory() {
    }

    private static List<Integer> intList(List<String> stringList) {
        List<Integer> list = new ArrayList<>();

        for (String s : stringList) {
            list.add(Integer.parseInt(s));
        }

        return list;
    }

    public static AggregatorGroup buildFromConfig(String streamName, String applicationName,
            KinesisClientLibConfiguration config, String configFile) throws Exception {
        List<ExternalConfigurationModel> models = ExternalConfigurationModel.buildFromConfig(configFile);

        if (models.size() == 0) {
            throw new InvalidConfigurationException(String.format(
                    "Unable to build any Aggregators from External Configuration %s", configFile));
        }

        AggregatorGroup aggregators = new AggregatorGroup();
        StreamAggregator agg = null;
        IDataExtractor dataExtractor = null;

        // the configuration may have included many configuration models
        for (ExternalConfigurationModel model : models) {
            switch (model.getDataExtractor()) {
                case CSV:
                    CsvDataExtractor d = new CsvDataExtractor(intList(model.getLabelItems())).withDateValueIndex(
                            Integer.parseInt(model.getDateItem())).withDelimiter(
                            model.getDelimiter()).withItemTerminator(model.getItemTerminator()).withRegexFilter(
                            model.getFilterRegex()).withDateFormat(model.getDateFormat()).withStringSummaryIndicies(
                            model.getSummaryItems());

                    if (model.getLabelAttributeAlias() != null) {
                        d.withLabelAttributeAlias(model.getLabelAttributeAlias());
                    }
                    if (model.getDateAttributeAlias() != null) {
                        d.withDateAttributeAlias(model.getDateAttributeAlias());
                    }

                    dataExtractor = d;
                    break;
                case REGEX:
                    RegexDataExtractor e = new RegexDataExtractor(model.getRegularExpression(),
                            intList(model.getLabelItems())).withItemTerminator(
                            model.getItemTerminator()).withDateValueIndex(
                            Integer.parseInt(model.getDateItem())).withDateFormat(
                            model.getDateFormat()).withStringSummaryIndicies(
                            model.getSummaryItems());
                    if (model.getLabelAttributeAlias() != null) {
                        e.withLabelAttributeAlias(model.getLabelAttributeAlias());
                    }
                    if (model.getDateAttributeAlias() != null) {
                        e.withDateAttributeAlias(model.getDateAttributeAlias());
                    }

                    dataExtractor = e;
                    break;
                case JSON:
                    dataExtractor = new JsonDataExtractor(model.getLabelItems())
                            .withDateFormat(model.getDateFormat())
                            .withDateValueAttribute(model.getDateItem())
                            .withSummaryAttributes(model.getSummaryItems())
                            .withItemTerminator(model.getItemTerminator())
                            .withRegexFilter(model.getFilterRegex())
                    ;
                    break;
                case OBJECT:
                    ObjectExtractor extractor = null;
                    if (model.isAnnotatedClass()) {
                        extractor = new ObjectExtractor(model.getClazz());
                    } else {
                        extractor = new ObjectExtractor(model.getLabelItems(), model.getClazz());
                    }

                    extractor.withDateMethod(model.getDateItem()).withSummaryMethods(
                            model.getSummaryItems());
                    dataExtractor = extractor;
                    break;

            }

            dataExtractor.setAggregatorType(model.getAggregatorType());

            agg = new StreamAggregator(streamName, applicationName, model.getNamespace(), config,
                    dataExtractor).withAggregatorType(model.getAggregatorType()).withStorageCapacity(
                    model.getReadIOPs(), model.getWriteIOPs()).withTableName(model.getTableName()).withTimeHorizon(
                    model.getTimeHorizons()).withRaiseExceptionOnDataExtractionErrors(
                    model.shouldFailOnDataExtraction());

            // configure metrics service on the aggregator if it's been
            // configured
            if (model.shouldEmitMetrics() || model.getMetricsEmitter() != null) {
                if (model.getMetricsEmitter() != null) {
                    agg.withMetricsEmitter(model.getMetricsEmitter().newInstance());
                } else {
                    agg.withCloudWatchMetrics();
                }
            }

            // create a new instance of the Data Store if one has been
            // configured. Currently we only support pluggable data stores that
            // are configured via their environment or have self defined
            // configuration models: only no args public constructors can be
            // called
            if (model.getDataStore() != null) {
                agg.withDataStore((IDataStore) model.getDataStore().newInstance());
            }

            aggregators.registerAggregator(agg);
        }

        return aggregators;
    }
}

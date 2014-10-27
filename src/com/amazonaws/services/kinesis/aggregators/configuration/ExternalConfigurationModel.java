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
package com.amazonaws.services.kinesis.aggregators.configuration;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.aggregators.AggregatorType;
import com.amazonaws.services.kinesis.aggregators.StreamAggregatorUtils;
import com.amazonaws.services.kinesis.aggregators.TimeHorizon;
import com.amazonaws.services.kinesis.aggregators.annotations.AnnotationProcessor;
import com.amazonaws.services.kinesis.aggregators.datastore.IDataStore;
import com.amazonaws.services.kinesis.aggregators.exception.ClassNotAnnotatedException;
import com.amazonaws.services.kinesis.aggregators.exception.InvalidConfigurationException;
import com.amazonaws.services.kinesis.aggregators.metrics.IMetricsEmitter;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ExternalConfigurationModel {
    private static final Log LOG = LogFactory.getLog(ExternalConfigurationModel.class);

    private String namespace;

    private List<TimeHorizon> timeHorizons;

    private AggregatorType aggregatorType;

    private DataExtractor dataExtractor;

    private List<String> labelItems = new ArrayList<>();

    private String labelAttributeAlias;

    private String dateItem, dateFormat, dateAttributeAlias;

    private List<String> summaryItems;

    private String delimiter;

    private String itemTerminator;

    private String filterRegex;

    private String regularExpression;

    private boolean isAnnotatedClass;

    private Class<?> clazz;

    private static ObjectMapper mapper = new ObjectMapper();

    private String tableName;

    private Long readIOPs;

    private Long writeIOPs;

    private boolean failOnDataExtraction;

    private boolean emitMetrics;

    private Class<IDataStore> dataStore;

    private Class<IMetricsEmitter> metricsEmitter;

    private static void configureCsv(JsonNode document, ExternalConfigurationModel model) {
        model.setDelimiter(StreamAggregatorUtils.readValueAsString(document, "delimiter"));
    }

    private static void configureStringCommon(JsonNode document, ExternalConfigurationModel model) {
        model.setItemTerminator(StreamAggregatorUtils.readValueAsString(document, "lineTerminator"));
        model.setFilterRegex(StreamAggregatorUtils.readValueAsString(document, "filterRegex"));
    }

    private static void configureRegex(JsonNode document, ExternalConfigurationModel model)
            throws InvalidConfigurationException {
        String regex = StreamAggregatorUtils.readValueAsString(document, "regularExpression");

        if (regex == null || regex.equals(""))
            throw new InvalidConfigurationException(
                    "Cannot configure a Regular Expression Aggregator without a Regular Expression (configuration 'regularExpression'");
        model.setRegularExpression(regex);
    }

    private static void configureObject(JsonNode document, ExternalConfigurationModel model)
            throws InvalidConfigurationException {

        String classname = StreamAggregatorUtils.readValueAsString(document, "class");
        if (classname == null || classname.equals(""))
            throw new InvalidConfigurationException(
                    "Cannot configure an Aggregator which uses Object based data extraction without a 'class' configuration item");

        try {
            model.setClazz(Class.forName(classname));
        } catch (ClassNotFoundException e) {
            throw new InvalidConfigurationException(String.format(
                    "ClassNotFoundException: %s not found on Classpath", classname));
        }

        // try to load the class using its annotations
        try {
            AnnotationProcessor p = new AnnotationProcessor(model.getClazz());
            model.setAnnotatedClass(true);
        } catch (ClassNotAnnotatedException e) {
            // no problem
        } catch (Exception e) {
            throw new InvalidConfigurationException(e);
        }
    }

    private static void addTimeHorizons(JsonNode document, ExternalConfigurationModel model)
            throws Exception {
        JsonNode node = StreamAggregatorUtils.readJsonValue(document, "timeHorizons");
        if (node != null) {
            Iterator<JsonNode> timeHorizonValues = node.elements();
            while (timeHorizonValues.hasNext()) {
                String t = timeHorizonValues.next().asText();
                String timeHorizonName = null;
                int granularity = -1;

                // process parameterised time horizons
                if (t.contains("MINUTES_GROUPED")) {
                    String[] items = t.split("\\(");
                    timeHorizonName = items[0];
                    granularity = Integer.parseInt(items[1].replaceAll("\\)", ""));
                } else {
                    timeHorizonName = t;
                }

                try {
                    TimeHorizon th = TimeHorizon.valueOf(timeHorizonName);

                    if (th.equals(TimeHorizon.MINUTES_GROUPED) && granularity == -1) {
                        throw new InvalidConfigurationException(
                                "Unable to create Grouped Minutes Time Horizon without configuration of Granularity using notation MINUTES_GROUPED(<granularity in minutes>)");
                    } else {
                        if (th.equals(TimeHorizon.MINUTES_GROUPED)) {
                            th.setGranularity(granularity);
                        }
                    }
                    model.addTimeHorizon(th);
                } catch (Exception e) {
                    throw new Exception(String.format("Unable to configure Time Horizon %s", t), e);
                }
            }
        }
    }

    private static void setAggregatorType(JsonNode document, ExternalConfigurationModel model)
            throws Exception {
        String aggType = StreamAggregatorUtils.readValueAsString(document, "type");

        if (aggType == null || aggType.equals("")) {
            model.setAggregatorType(AggregatorType.COUNT);
        } else {
            try {
                model.setAggregatorType(AggregatorType.valueOf(aggType));
            } catch (Exception e) {
                throw new Exception(String.format("Unable to configure AggregatorType %s", aggType));
            }
        }
    }

    public static List<ExternalConfigurationModel> buildFromConfig(String configFilePath)
            throws Exception {
        List<ExternalConfigurationModel> response = new ArrayList<>();

        // reference the config file as a full path
        File configFile = new File(configFilePath);
        if (!configFile.exists()) {

            // try to load the file from the classpath
            InputStream classpathConfig = ExternalConfigurationModel.class.getClassLoader().getResourceAsStream(
                    configFilePath);
            if (classpathConfig != null && classpathConfig.available() > 0) {
                configFile = new File(ExternalConfigurationModel.class.getResource(
                        (configFilePath.startsWith("/") ? "" : "/") + configFilePath).toURI());

                LOG.info(String.format("Loaded Configuration %s from Classpath", configFilePath));
            } else {
                if (configFilePath.startsWith("s3://")) {
                    AmazonS3 s3Client = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());
                    TransferManager tm = new TransferManager(s3Client);

                    // parse the config path to get the bucket name and prefix
                    final String s3ProtoRegex = "s3:\\/\\/";
                    String bucket = configFilePath.replaceAll(s3ProtoRegex, "").split("/")[0];
                    String prefix = configFilePath.replaceAll(
                            String.format("%s%s\\/", s3ProtoRegex, bucket), "");

                    // download the file using TransferManager
                    configFile = File.createTempFile(configFilePath, null);
                    Download download = tm.download(bucket, prefix, configFile);
                    download.waitForCompletion();

                    // shut down the transfer manager
                    tm.shutdownNow();

                    LOG.info(String.format("Loaded Configuration from Amazon S3 %s/%s to %s",
                            bucket, prefix, configFile.getAbsolutePath()));
                } else {
                    // load the file from external URL
                    try {
                        configFile = File.createTempFile(configFilePath, null);
                        FileUtils.copyURLToFile(new URL(configFilePath), configFile, 1000, 1000);
                        LOG.info(String.format("Loaded Configuration from %s to %s",
                                configFilePath, configFile.getAbsolutePath()));
                    } catch (IOException e) {
                        // handle the timeouts and so on with a generalised
                        // config
                        // file not found handler later
                    }
                }
            }
        } else {
            LOG.info(String.format("Loaded Configuration from Filesystem %s", configFilePath));
        }

        // if we haven't been able to load a config file, then bail
        if (configFile == null || !configFile.exists()) {
            throw new InvalidConfigurationException(String.format(
                    "Unable to Load Config File from %s", configFilePath));
        }

        JsonNode document = StreamAggregatorUtils.asJsonNode(configFile);

        ExternalConfigurationModel config = null;

        Iterator<JsonNode> i = document.elements();
        while (i.hasNext()) {
            config = new ExternalConfigurationModel();

            JsonNode section = i.next();

            // set generic properties
            config.setNamespace(StreamAggregatorUtils.readValueAsString(section, "namespace"));
            config.setDateFormat(StreamAggregatorUtils.readValueAsString(section, "dateFormat"));
            addTimeHorizons(section, config);
            setAggregatorType(section, config);

            // set the label items
            JsonNode labelItems = StreamAggregatorUtils.readJsonValue(section, "labelItems");
            if (labelItems != null && labelItems.size() > 0) {
                Iterator<JsonNode> iterator = labelItems.elements();
                while (iterator.hasNext()) {
                    JsonNode n = iterator.next();
                    config.addLabelItems(n.asText());
                }
            }
            config.setLabelAttributeAlias(StreamAggregatorUtils.readValueAsString(section,
                    "labelAttributeAlias"));

            config.setDateItem(StreamAggregatorUtils.readValueAsString(section, "dateItem"));
            config.setDateAttributeAlias(StreamAggregatorUtils.readValueAsString(section,
                    "dateAttributeAlias"));
            JsonNode summaryItems = StreamAggregatorUtils.readJsonValue(section, "summaryItems");
            if (summaryItems != null && summaryItems.size() > 0) {
                Iterator<JsonNode> iterator = summaryItems.elements();
                while (iterator.hasNext()) {
                    JsonNode n = iterator.next();
                    config.addSummaryItem(n.asText());
                }
            }

            config.setTableName(StreamAggregatorUtils.readValueAsString(section, "tableName"));

            String readIO = StreamAggregatorUtils.readValueAsString(section, "readIOPS");
            if (readIO != null)
                config.setReadIOPs(Long.parseLong(readIO));
            String writeIO = StreamAggregatorUtils.readValueAsString(section, "writeIOPS");
            if (writeIO != null)
                config.setWriteIOPs(Long.parseLong(writeIO));

            // configure tolerance of data extraction problems
            String failOnDataExtraction = StreamAggregatorUtils.readValueAsString(section,
                    "failOnDataExtraction");
            if (failOnDataExtraction != null)
                config.setFailOnDataExtraction(Boolean.parseBoolean(failOnDataExtraction));

            // configure whether metrics should be emitted
            String emitMetrics = StreamAggregatorUtils.readValueAsString(section, "emitMetrics");
            String metricsEmitterClassname = StreamAggregatorUtils.readValueAsString(section,
                    "metricsEmitterClass");
            if (emitMetrics != null || metricsEmitterClassname != null) {
                if (metricsEmitterClassname != null) {
                    config.setMetricsEmitter((Class<IMetricsEmitter>) ClassLoader.getSystemClassLoader().loadClass(
                            metricsEmitterClassname));
                } else {
                    config.setEmitMetrics(Boolean.parseBoolean(emitMetrics));
                }
            }

            // configure the data store class
            String dataStoreClass = StreamAggregatorUtils.readValueAsString(section, "IDataStore");
            if (dataStoreClass != null) {
                Class<IDataStore> dataStore = (Class<IDataStore>) ClassLoader.getSystemClassLoader().loadClass(
                        dataStoreClass);
                config.setDataStore(dataStore);
            }

            // get the data extractor configuration, so we know what other json
            // elements to retrieve from the configuration document
            String useExtractor = null;
            try {
                useExtractor = StreamAggregatorUtils.readValueAsString(section, "dataExtractor");
                config.setDataExtractor(DataExtractor.valueOf(useExtractor));
            } catch (Exception e) {
                throw new Exception(String.format(
                        "Unable to configure aggregator with Data Extractor %s", useExtractor));
            }

            switch (config.getDataExtractor()) {
                case CSV:
                    configureStringCommon(section, config);
                    configureCsv(section, config);
                    break;
                case JSON:
                    configureStringCommon(section, config);
                    break;
                case OBJECT:
                    configureObject(section, config);
                    break;
                case REGEX:
                    configureRegex(section, config);
            }

            response.add(config);
        }
        return response;
    }

    public String getNamespace() {
        return this.namespace;
    }

    public List<TimeHorizon> getTimeHorizons() {
        return this.timeHorizons;
    }

    public String getFilterRegex() {
        return this.filterRegex;
    }

    public String getRegularExpression() {
        return this.regularExpression;
    }

    public String getTableName() {
        return this.tableName;
    }

    public Long getReadIOPs() {
        return this.readIOPs;
    }

    public Long getWriteIOPs() {
        return this.writeIOPs;
    }

    public String getLabelAttributeAlias() {
        return this.labelAttributeAlias;
    }

    public String getDateAttributeAlias() {
        return this.dateAttributeAlias;
    }

    public boolean isAnnotatedClass() {
        return this.isAnnotatedClass;
    }

    public void addTimeHorizon(TimeHorizon timeHorizon) {
        if (this.timeHorizons == null)
            this.timeHorizons = new ArrayList<>();

        this.timeHorizons.add(timeHorizon);
    }

    public AggregatorType getAggregatorType() {
        return this.aggregatorType;
    }

    public List<String> getLabelItems() {
        return this.labelItems;
    }

    public String getDateItem() {
        return this.dateItem;
    }

    public String getDateAlias() {
        return this.dateAttributeAlias;
    }

    public String getDateFormat() {
        return this.dateFormat;
    }

    public List<String> getSummaryItems() {
        return this.summaryItems;
    }

    public String getDelimiter() {
        return this.delimiter;
    }

    public String getItemTerminator() {
        return this.itemTerminator;
    }

    public void addSummaryItem(String summaryItem) {
        if (this.summaryItems == null)
            this.summaryItems = new ArrayList<>();

        this.summaryItems.add(summaryItem);
    }

    public Class getClazz() {
        return this.clazz;
    }

    public DataExtractor getDataExtractor() {
        return this.dataExtractor;
    }

    public boolean shouldFailOnDataExtraction() {
        return this.failOnDataExtraction;
    }

    public boolean shouldEmitMetrics() {
        return this.emitMetrics;
    }

    public Class<IMetricsEmitter> getMetricsEmitter() {
        return this.metricsEmitter;
    }

    public Class getDataStore() {
        return this.dataStore;
    }

    private void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    private void setAggregatorType(AggregatorType aggregatorType) {
        this.aggregatorType = aggregatorType;
    }

    private void addLabelItems(String labelItem) {
        this.labelItems.add(labelItem);
    }

    private void setLabelItems(List<String> labelItems) {
        this.labelItems = labelItems;
    }

    private void setDateItem(String dateItem) {
        this.dateItem = dateItem;
    }

    private void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

    private void setDelimiter(String delimiter) {
        if (delimiter != null && !delimiter.equals(""))
            this.delimiter = delimiter;
    }

    private void setItemTerminator(String itemTerminator) {
        if (itemTerminator != null && !itemTerminator.equals(""))
            this.itemTerminator = itemTerminator;
    }

    private void setFilterRegex(String filterRegex) {
        this.filterRegex = filterRegex;
    }

    private void setRegularExpression(String regularExpression) {
        this.regularExpression = regularExpression;
    }

    private void setClazz(Class clazz) {
        this.clazz = clazz;
    }

    private void setDataExtractor(DataExtractor dataExtractor) {
        this.dataExtractor = dataExtractor;
    }

    private void setAnnotatedClass(boolean isAnnotatedClass) {
        this.isAnnotatedClass = isAnnotatedClass;
    }

    private void setTableName(String tableName) {
        if (tableName != null && !tableName.equals(""))
            this.tableName = tableName;
    }

    private void setReadIOPs(Long readIOPs) {
        this.readIOPs = readIOPs;
    }

    private void setWriteIOPs(Long writeIOPs) {
        this.writeIOPs = writeIOPs;
    }

    private void setFailOnDataExtraction(boolean failOnDataExtraction) {
        this.failOnDataExtraction = failOnDataExtraction;
    }

    private void setEmitMetrics(boolean emitMetrics) {
        this.emitMetrics = emitMetrics;
    }

    private void setMetricsEmitter(Class<IMetricsEmitter> metricsEmitter) {
        this.metricsEmitter = metricsEmitter;
    }

    private void setDataStore(Class<IDataStore> dataStore) {
        this.dataStore = dataStore;
    }

    private void setLabelAttributeAlias(String labelAttributeAlias) {
        this.labelAttributeAlias = labelAttributeAlias;
    }

    private void setDateAttributeAlias(String dateAttributeAlias) {
        this.dateAttributeAlias = dateAttributeAlias;
    }
}

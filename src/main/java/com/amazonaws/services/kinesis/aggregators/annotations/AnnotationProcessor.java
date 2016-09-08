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

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.kinesis.aggregators.AggregatorType;
import com.amazonaws.services.kinesis.aggregators.LabelSet;
import com.amazonaws.services.kinesis.aggregators.StreamAggregatorUtils;
import com.amazonaws.services.kinesis.aggregators.TimeHorizon;
import com.amazonaws.services.kinesis.aggregators.datastore.IDataStore;
import com.amazonaws.services.kinesis.aggregators.exception.ClassNotAnnotatedException;
import com.amazonaws.services.kinesis.aggregators.exception.InvalidConfigurationException;
import com.amazonaws.services.kinesis.aggregators.metrics.IMetricsEmitter;
import com.amazonaws.services.kinesis.aggregators.summary.SummaryCalculation;
import com.amazonaws.services.kinesis.aggregators.summary.SummaryConfiguration;
import com.amazonaws.services.kinesis.aggregators.summary.SummaryElement;

/**
 * AnnotationProcess provides a helper mechanism to extract information from an
 * Annotationed Class which will be used to configure an Object Serialisation
 * based Aggregation. See
 * {@link com.amazonaws.services.kinesis.aggregators.factory.ObjectAggregatorFactory}
 * .
 */
public class AnnotationProcessor {
    @SuppressWarnings("rawtypes")
    private Class clazz;

    private LabelSet labelSet = new LabelSet();

    private List<String> labelMethodNames = new ArrayList<>();

    private Map<String, Method> labelMethodMap = new LinkedHashMap<>();

    private String dateMethodName;

    private Method dateMethod;

    private Map<String, Method> summaryMethods = new HashMap<>();

    private SummaryConfiguration summaryConfig = new SummaryConfiguration();

    private AggregatorType type;

    private List<TimeHorizon> timeHorizons;

    private boolean timeHierarchy;

    private String namespace;

    private boolean failOnDataExtractionErrors = true;

    private boolean emitMetrics = false;

    private Class<IDataStore> dataStore;

    private Class<IMetricsEmitter> metricsEmitter;

    private AnnotationProcessor() {
    }

    /**
     * Create a new Annotation Processor for an Annotated Class.
     * 
     * @param clazz The Class to extract annotation information from.
     * @throws Exception
     */
    public AnnotationProcessor(@SuppressWarnings("rawtypes") Class clazz) throws Exception {
        this.clazz = clazz;
        boolean isAnnotated = false;

        // get the class annotations
        for (Annotation a : this.clazz.getAnnotations()) {
            if (a.annotationType().equals(Aggregate.class)) {
                isAnnotated = true;

                Aggregate annotatedObject = (Aggregate) a;
                this.namespace = annotatedObject.namespace();
                if (this.namespace.contains(" "))
                    throw new ClassNotAnnotatedException("Namespace may not contain spaces");

                this.type = annotatedObject.type();

                // process time horizon annotations
                int[] timeGranularities = annotatedObject.timeGranularity();
                TimeHorizon[] horizons = annotatedObject.timeHorizons();
                this.timeHorizons = new ArrayList<>();
                int i = 0;
                for (TimeHorizon h : horizons) {
                    if (h.equals(TimeHorizon.MINUTES_GROUPED)) {
                        try {
                            // prevent use of the default time granularity
                            if (timeGranularities[i] == -1) {
                                throw new ArrayIndexOutOfBoundsException();
                            }
                            h.setGranularity(timeGranularities[i]);
                        } catch (ArrayIndexOutOfBoundsException e) {
                            throw new InvalidConfigurationException(
                                    "Unable to generate a MINUTES_GROUPED Time Horizon without configuration of timeGranularity");
                        }
                    }
                    this.timeHorizons.add(h);
                    i++;
                }

                this.failOnDataExtractionErrors = annotatedObject.failOnDataExtractionErrors();

                this.emitMetrics = annotatedObject.emitMetrics();

                this.dataStore = annotatedObject.dataStore();

                this.metricsEmitter = annotatedObject.metricsEmitter();
            }
        }

        if (!isAnnotated)
            throw new ClassNotAnnotatedException(
                    "Cannot get Aggregator Config from non-Annotated Class");

        // process the method annotations
        if (isAnnotated) {
            for (Method m : this.clazz.getDeclaredMethods()) {
                // label method
                if (m.getAnnotation(Label.class) != null) {
                    this.labelMethodNames.add(m.getName());
                    m.setAccessible(true);
                    this.labelMethodMap.put(m.getName(), m);

                    this.labelSet.put(m.getName(), null);
                }

                // date method
                if (m.getAnnotation(DateValue.class) != null) {
                    this.dateMethodName = m.getName();
                    m.setAccessible(true);
                    this.dateMethod = m;
                }

                // summary methods
                Annotation summary = m.getAnnotation(Summary.class);
                if (summary != null) {
                    m.setAccessible(true);
                    this.summaryMethods.put(m.getName(), m);

                    // process the summary configuration
                    SummaryCalculation[] requestedCalcs = ((Summary) summary).type();

                    if (requestedCalcs != null) {
                        for (SummaryCalculation c : requestedCalcs) {
                            this.summaryConfig.add(m.getName(), new SummaryElement(m.getName(), c));
                        }
                    } else {
                        this.summaryConfig.add(m.getName(), new SummaryElement(m.getName(),
                                SummaryCalculation.SUM));
                    }
                }
            }
        }
    }

    public List<String> getLabelMethodNames() {
        return this.labelMethodNames;
    }

    public Map<String, Method> getLabelMethods() {
        return this.labelMethodMap;
    }

    public String getDateMethodName() {
        return this.dateMethodName;
    }

    public Method getDateMethod() {
        return this.dateMethod;
    }

    public Map<String, Method> getSummaryMethods() {
        return this.summaryMethods;
    }

    public SummaryConfiguration getSummaryConfig() {
        return this.summaryConfig;
    }

    public AggregatorType getType() {
        return this.type;
    }

    public List<TimeHorizon> getTimeHorizon() {
        return this.timeHorizons;
    }

    public boolean hasTimeHierarchy() {
        return this.timeHierarchy;
    }

    public boolean shouldFailOnDataExtractionErrors() {
        return this.failOnDataExtractionErrors;
    }

    public boolean shouldEmitMetrics() {
        return this.emitMetrics;
    }

    public Class<IMetricsEmitter> getMetricsEmitter() {
        return this.metricsEmitter;
    }

    public Class<IDataStore> getDataStore() {
        return this.dataStore;
    }

    public String getNamespace() {
        return this.namespace;
    }
}

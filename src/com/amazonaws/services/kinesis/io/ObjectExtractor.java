package com.amazonaws.services.kinesis.io;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.aggregators.AggregateData;
import com.amazonaws.services.kinesis.aggregators.AggregatorType;
import com.amazonaws.services.kinesis.aggregators.InputEvent;
import com.amazonaws.services.kinesis.aggregators.LabelSet;
import com.amazonaws.services.kinesis.aggregators.StreamAggregator;
import com.amazonaws.services.kinesis.aggregators.StreamAggregatorUtils;
import com.amazonaws.services.kinesis.aggregators.annotations.AnnotationProcessor;
import com.amazonaws.services.kinesis.aggregators.exception.InvalidConfigurationException;
import com.amazonaws.services.kinesis.aggregators.exception.SerializationException;
import com.amazonaws.services.kinesis.aggregators.exception.UnsupportedCalculationException;
import com.amazonaws.services.kinesis.aggregators.summary.SummaryConfiguration;
import com.amazonaws.services.kinesis.aggregators.summary.SummaryElement;
import com.amazonaws.services.kinesis.io.serializer.IKinesisSerializer;
import com.amazonaws.services.kinesis.io.serializer.JsonSerializer;

/**
 * IDataExtractor which supports extracting data from Objects via reflected
 * method signatures.
 */
public class ObjectExtractor extends AbstractDataExtractor implements IDataExtractor {
    @SuppressWarnings("rawtypes")
    private Class clazz;

    private String uniqueIdMethodName;

    private List<String> aggregateLabelMethods;

    private Map<String, Method> aggregateLabelMethodMap = new LinkedHashMap<>();

    private String aggregateLabelColumn, dateValueColumn, dateMethodName;

    private Method dateMethod, uniqueIdMethod;

    private Object eventDate;

    private Map<String, Method> sumValueMap;

    private Object summaryValue;

    private final Log LOG = LogFactory.getLog(ObjectExtractor.class);

    private Date dateValue;

    private Map<String, Double> sumUpdates = new HashMap<>();

    private IKinesisSerializer<Object, byte[]> serialiser;

    private List<AggregateData> data;

    private boolean validated = false;

    private ObjectExtractor() {
    }

    public ObjectExtractor(Class clazz) throws Exception {
        AnnotationProcessor p = new AnnotationProcessor(clazz);
        this.aggregateLabelMethods = p.getLabelMethodNames();

        for (String s : p.getLabelMethodNames()) {
            this.aggregateLabelMethodMap.put(s, p.getLabelMethods().get(s));
        }

        LabelSet labels = LabelSet.fromStringKeys(this.aggregateLabelMethods);
        this.aggregateLabelColumn = StreamAggregatorUtils.methodToColumn(labels.getName());
        this.dateMethodName = p.getDateMethodName();
        this.dateValueColumn = StreamAggregatorUtils.methodToColumn(p.getDateMethodName());
        this.dateMethod = p.getDateMethod();
        this.sumValueMap = p.getSummaryMethods();
        this.summaryConfig = p.getSummaryConfig();

        this.clazz = clazz;
        this.serialiser = new JsonSerializer(clazz);
    }

    /**
     * Create an Object Extractor using Default serialisation for the class.
     * 
     * @param aggregateLabelMethod The method to be used as the label for
     *        aggregation.
     * @param clazz The base class used for deserialisation and accessed using
     *        configured accessor methods.
     */
    public ObjectExtractor(List<String> aggregateLabelMethods, Class clazz) throws Exception {
        this(aggregateLabelMethods, clazz, null);

    }

    /**
     * Create an Object Extractor using indicated serialisation for the class.
     * 
     * @param aggregateLabelMethod The method to be used as the label for
     *        aggregation.
     * @param clazz The base class used for deserialisation and accessed using
     *        configured accessor methods.
     * @param serialiser Instance of an ITransformer which converts between the
     *        binary Kinesis format and the required Object format indicated by
     *        the base class.
     */
    public ObjectExtractor(List<String> aggregateLabelMethodNames, Class clazz,
            IKinesisSerializer<Object, byte[]> serialiser) throws Exception {
        this.clazz = clazz;

        if (serialiser == null) {
            this.serialiser = new JsonSerializer(clazz);
        } else {
            this.serialiser = serialiser;
        }

        if (aggregateLabelMethodNames == null || aggregateLabelMethodNames.size() == 0) {
            throw new InvalidConfigurationException(
                    "Cannot Aggregate an Object without a Label Method");
        } else {
            this.aggregateLabelMethods = aggregateLabelMethodNames;

            for (String s : aggregateLabelMethodNames) {
                Method m = clazz.getDeclaredMethod(s);
                m.setAccessible(true);

                this.aggregateLabelMethodMap.put(s, m);
            }
        }

        LabelSet labels = LabelSet.fromStringKeys(this.aggregateLabelMethods);
        this.aggregateLabelColumn = labels.getName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void validate() throws Exception {
        if (!validated) {
            // validate sum config
            if ((this.aggregatorType.equals(AggregatorType.SUM)) && this.sumValueMap == null) {
                throw new Exception(
                        "Summary Aggregators require both a Label Field and a Value Field Set");
            }

            if (this.aggregatorType.equals(AggregatorType.SUM)) {
                for (String s : this.sumValueMap.keySet()) {
                    try {
                        Method m = clazz.getDeclaredMethod(s);
                        m.setAccessible(true);
                        this.sumValueMap.put(s, m);
                    } catch (NoSuchMethodException e) {
                        LOG.error(e);
                        throw e;
                    }
                }
            }

            LOG.info(String.format("Object Extractor Configuration\n" + "Class: %s\n"
                    + "Date Method: %s\n", this.clazz.getSimpleName(), this.dateMethodName));

            validated = true;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AggregateData> getData(InputEvent event) throws SerializationException {
        if (!validated) {
            try {
                validate();
            } catch (Exception e) {
                throw new SerializationException(e);
            }
        }

        try {
            List<AggregateData> data = new ArrayList<>();

            Object o = serialiser.toClass(event);

            // get the value of the reflected labels
            LabelSet labels = new LabelSet();
            for (String key : this.aggregateLabelMethods) {
                labels.put(key, aggregateLabelMethodMap.get(key).invoke(o).toString());
            }

            // get the unique ID value from the object
            String uniqueId = null;
            if (this.uniqueIdMethodName != null) {
                switch (this.uniqueIdMethodName) {
                    case StreamAggregator.REF_PARTITION_KEY:
                        uniqueId = event.getPartitionKey();
                        break;
                    case StreamAggregator.REF_SEQUENCE:
                        uniqueId = event.getSequenceNumber();
                        break;
                    default:
                        Object id = uniqueIdMethod.invoke(o);
                        if (id != null) {
                            uniqueId = id.toString();
                        }
                        break;
                }
            }

            // get the date value from the object
            if (this.dateMethod != null) {
                eventDate = dateMethod.invoke(o);

                if (eventDate == null) {
                    dateValue = new Date(System.currentTimeMillis());
                } else {
                    if (eventDate instanceof Date) {
                        dateValue = (Date) eventDate;
                    } else if (eventDate instanceof Long) {
                        dateValue = new Date((Long) eventDate);
                    } else {
                        throw new Exception(String.format(
                                "Cannot use data type %s for date value on event",
                                eventDate.getClass().getSimpleName()));
                    }
                }
            }

            // extract all summed values from the serialised object
            if (this.aggregatorType.equals(AggregatorType.SUM)) {
                // lift out the aggregated method value
                for (String s : this.sumValueMap.keySet()) {
                    summaryValue = this.sumValueMap.get(s).invoke(o);

                    if (summaryValue != null) {
                        if (summaryValue instanceof Double) {
                            sumUpdates.put(s, (Double) summaryValue);
                        } else if (summaryValue instanceof Long) {
                            sumUpdates.put(s, ((Long) summaryValue).doubleValue());
                        } else if (summaryValue instanceof Integer) {
                            sumUpdates.put(s, ((Integer) summaryValue).doubleValue());
                        } else {
                            String msg = String.format(
                                    "Unable to access  Summary %s due to NumberFormatException", s);
                            LOG.error(msg);
                            throw new SerializationException(msg);
                        }
                    }
                }
            }

            data.add(new AggregateData(uniqueId, labels, dateValue, sumUpdates));

            return data;
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }

    /**
     * Builder which allows for configuration a date method to be used as the
     * date item for aggregation.
     * 
     * @param dateMethodName The name of the method which returns the date for
     *        the event.
     * @return
     */
    public ObjectExtractor withDateMethod(String dateMethodName) throws NoSuchMethodException {
        this.dateMethodName = dateMethodName;
        this.dateValueColumn = StreamAggregatorUtils.methodToColumn(dateMethodName);
        this.dateMethod = this.clazz.getDeclaredMethod(dateMethodName);
        this.dateMethod.setAccessible(true);
        return this;
    }

    public ObjectExtractor withUniqueIdMethod(String uniqueIdMethodName)
            throws NoSuchMethodException {
        this.uniqueIdMethodName = uniqueIdMethodName;

        switch (this.uniqueIdMethodName) {
            case StreamAggregator.REF_PARTITION_KEY:
                break;
            case StreamAggregator.REF_SEQUENCE:
                break;
            default:
                this.uniqueIdMethod = this.clazz.getDeclaredMethod(this.uniqueIdMethodName);
                break;
        }

        return this;
    }

    /**
     * Builder which allows associating a set of method names or expressions
     * against methods for use as summary aggregate values.
     * 
     * @param summaryMethodName The method name or an expression against the
     *        method name which will be used as summary aggregate values. For
     *        instance, when an expression is used against a method, the format
     *        is SummaryCalculation(methodName), for example:
     *        sum(getObjectValue)
     * @return
     * @throws UnsupportedCalculationException
     */
    public ObjectExtractor withSummaryMethods(List<String> summaryMethodName)
            throws UnsupportedCalculationException {
        if (summaryMethodName != null) {
            this.aggregatorType = AggregatorType.SUM;

            if (this.sumValueMap == null)
                this.sumValueMap = new HashMap<>();

            for (String s : summaryMethodName) {
                this.summaryConfig.withConfigItem(s);
                // parse the requested summary method name into a calculation
                // and name, as we require the method name directly
                SummaryElement e = new SummaryElement(s);
                this.sumValueMap.put(e.getStreamDataElement(), null);
            }
        }

        return this;
    }

    public ObjectExtractor withSummaryConfig(SummaryConfiguration config) {
        this.summaryConfig = config;

        if (this.sumValueMap == null)
            this.sumValueMap = new HashMap<>();

        for (String s : this.summaryConfig.getItemSet()) {
            this.sumValueMap.put(s, null);
        }

        return this;
    }

    /**
     * Get the class which this object can extract data from
     * 
     * @return
     */
    @SuppressWarnings("rawtypes")
    public Class getClazz() {
        return this.clazz;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getAggregateLabelName() {
        return this.aggregateLabelColumn;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDateValueName() {
        return this.dateValueColumn == null ? StreamAggregator.DEFAULT_DATE_VALUE
                : this.dateValueColumn;
    }

    public IDataExtractor copy() throws Exception {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getUniqueIdName() {
        if (this.uniqueIdMethod != null) {
            return StreamAggregatorUtils.methodToColumn(this.uniqueIdMethodName);
        } else {
            return null;
        }
    }
}

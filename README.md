Amazon Kinesis Aggregators

Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

http://aws.amazon.com/asl/

Amazon Kinesis Aggregators is a Java framework that enables the automatic creation of real-time aggregated time series data from Amazon Kinesis streams. 

You can use this data to answer questions such as ‘how many times per second has ‘x’ occurred’ or ‘what was the breakdown by hour over the day of the streamed data containing ‘y'. Using this framework, you simply describe the format of the data on your stream (CSV, JSON, and so on), the granularity of times series that you require (seconds, minutes, hours, and so on), and how the data elements that are streamed should be grouped; the framework handles all the time series calculations and data persistence. You then simply consume the time series aggregates in your application using Amazon DynamoDB, or interact with the time series using Amazon CloudWatch or the Web Query API. 

You can also analyze the data using Hive on Amazon Elastic MapReduce, or bulk import it to Amazon Redshift. The process runs as a standalone Amazon Kinesis-enabled application which only requires configuration, or can be integrated into existing Amazon Kinesis applications.

The data is stored in a time series based on how you aggregate it. A dataset aggregating Telecoms Call Data Records in DynamoDB might look like this:

![Dynamo Real Time Aggregate Table](https://s3.amazonaws.com/amazon-kinesis-aggregators/img/DynamoTable.png)

The corresponding data in CloudWatch would look like this:

![CloudWatch Dashboard View](https://s3.amazonaws.com/amazon-kinesis-aggregators/img/CloudWatch.png)

## Building Aggregators

Amazon Kinesis Aggregators is built using Apache Maven. To build, simply run Maven from the amazon-kinesis-aggregators directory. The target directory contains the following build artifacts:

* **amazon-kinesis-aggregators-.9.2.7.4.jar** - Includes no compiled dependencies
* **AmazonKinesisAggregators.jar-complete.jar** - Includes all required dependencies
* **AmazonKinesisAggregator.war** - The web application archive file

## Running Aggregators

Amazon Kinesis Aggregators ships with several deployment options, which should enable you to run with minimal operational overhead while also accommodating advanced deployment use cases. You can run Amazon Kinesis Aggregators as:

* A fully-managed Elastic Beanstalk application. All you need to do is deploy the KinesisAggregators.war file, and provide a configuration file that is accessible using HTTP.
* A managed Java client, running through the host orchestration of your choice. For example, you can deploy this managed Java client as part of an Amazon EC2 fleet that uses Auto Scaling.
* As part of an existing Amazon Kinesis-enabled application. This enables an existing application to 'sideload' aggregator processing, as an augmentation to an already-established application.

### Running Amazon Kinesis Aggregators Using Elastic Beanstalk

Amazon Kinesis Aggregators compiles a web application archive (WAR) file, which enables easy deployment on Java application servers, such as Apache Tomcat, using Elastic Beanstalk (http://aws.amazon.com/elasticbeanstalk). Amazon Kinesis Aggregators also includes configuration options that instruct Elastic Beanstalk to scale the application on CPU load, which is typically the bottleneck for applications as they scale up. This is the recommended deployment method.

To deploy Amazon Kinesis Aggregators as an Elastic Beanstalk application, start by creating a new Elastic Beanstalk web server application with the pre-configured Tomcat stack. When prompted by the AWS Management Console, upload the KinesisAggregators.war file from your local build. Select an instance type that is suitable for the type of aggregation that you are running (specifically, the higher the granularity of label items and the more fine-grained the TimeHorizon value, the larger the instance type you require). After deployment, click the URL for the application environment; the following message is displayed:

```OK - Kinesis Aggregators Managed Application hosted in Elastic Beanstalk Online ```

Furthermore, if you request a log snapshot from the Elastic Beanstalk console, you see a log line indicating the following:

```No Aggregators Configuration File found in Beanstalk Configuration config-file-url. Application is Idle```

This indicates that the application is deployed but not configured. To configure the application, add these Elastic Beanstalk configuration parameters as required: 

* **stream-name** - The name of the stream.
* **application-name** - The name of the Amazon Kinesis application.
* **failures-tolerated** - The number of worker exceptions allowed before the worker terminates.
* **position-in-stream** - The position in the stream to start consuming data from. The possible values are 'LATEST' and 'TRIM_HORIZON'.
* **max-records** - The maximum number of records to consume from a stream in a single cycle. You can set this value if your stream processing (in addition to aggregation) is slow.
* **region** - The region to use for the stream, the DynamoDB lease tables, and the CloudWatch and aggregate data stores. Amazon Kinesis Aggregators does not currently support cross-region deployment.
* **environment** - The name of the environment. This ensures that all DynamoDB tables are prefixed with the environment, enabling you to keep data sets separate for test and production (for example).
* **config-file-url** - The URL for the configuration file.

This is typically done by adding `-D` flags to the JVM command line options. Then, choose 'Save' and Elastic Beanstalk applies the changes to the environment. Wait a minute or so, and then snapshot logs to confirm that Amazon Kinesis Aggregators is running.

### Running the Managed Java Client Application

This is a great option if you have data in Amazon Kinesis, but don’t want to use Elastic Beanstalk. You can start the application from a server using the following command:

```java -cp AmazonKinesisAggregators.jar-complete.jar -Dconfig-file-path=<configuration> -Dstream-name=<stream name> -Dapplication-name=<application name> -Dregion=<region name - us-east-1, eu-west-1, etc> com.amazonaws.services.kinesis.aggregators.consumer.AggregatorConsumer```

In addition to the configuration items outlined in the Elastic Beanstalk section, use the following configuration item:

* **config-file-path** - The path to the configuration file.

We recommend that you run your servers in an Auto Scaling group to ensure fault tolerance if the host fails.

### Configuration

You can use the configuration file to create one or more aggregations against the same stream. It is a JSON file that creates a set of aggregator objects managed by the framework. Create one aggregator for each distinct label that you want to aggregate on. Each aggregator can then have its own properties of time granularity, aggregator type, and so on.

The core structure of the configuration file is an array of aggregator objects. For example, the following configuration creates two aggregators:

```[{aggregatorDef1}, {aggregatorDef2}]```

Note that aggregatorDef*N* is an aggregator configuration. An aggregator configuration must include the following attributes:

* **namespace** (String) - Enables you to create separate time series data stores. This namespace is used with the application name and environment to create the underlying data tables for the time series, as well as the namespace for custom CloudWatch metrics. Use something that's meaningful based upon the label and time granularity.
* **labelItems** (array&lt;String&gt;) - Includes a list of the elements of the data stream to aggregate on. The data stored in the time series is aggregated by the unique values from the stream for these attributes, and by time. For instance, to aggregate data for searches made against a car website, you might have a label item set of ["Make","Model","Year"]. If you are using CSV data, then this same configuration might be positional based on the fields in the line, such as [0,3,5].
* **labelAttributeAlias** (String) - Enables you to name the target database attribute for the label. This is particularly useful when you are using CSV or regex-extracted data, and would otherwise end up with a label attribute named the same as the label attribute index.
* **type** (enum) - The type of aggregation to run. The available types are 'COUNT' and 'SUM'. Counting aggregators simply counts the instances of unique values in Label Items by time. Using the previous example, it would generate a count of searches by configured time period for each unique combination of Make, Model, and Year. Building on this 'SUM' type, aggregators also calculate summaries of other numeric values on the stream. For more information, see the configuration option **'summaryItems**'.
* **timeHorizons** (array&lt;enum&gt;) - Because the data is captured as a time series, you must tell the aggregator which definition of time you require. To have the data on the stream aggregated by minute, specify 'MINUTE'. To put data into buckets of 5 minutes duration, specify MINUTES_GROUPED(5). You can specify multiple timeHorizon values, and the aggregator automatically maintains the time series data at that granularity. A common configuration might be ["SECOND","HOUR","FOREVER"], which gives per-second aggregates, a rollup by Hour, and a simple data set to view everything that ever occurred in a single value. The possible values are:
  * SECOND
  * MINUTE
  * MINUTES\_GROUPED(int minutePeriod) - Groups data into time buckets using a minute period. For 4 buckets per hour, use '15', or use '5' for buckets of aggregation that are 5 minutes long.
  * HOUR
  * DAY
  * MONTH
  * YEAR
  * FOREVER - Rolls up everything that occurred in a single value '*'. 
* **dataExtractor** (enum) - Tells the aggregator how to parse and extract the Label Items from your stream. Currently, the following data formats are supported for external configurations using the configuration file:
  * **CSV** - Character-separated UTF-8 data. The default delimiter is a comma. To override the delimiter, set the configuration option 'delimiter' to the character value to use as the field terminator. Also, note that all data extractors support multi-value events. This means that you can have many CSV 'lines' within a single event, which are extracted with a line terminator of "\n". To override the line terminator on any data extractor that is text-based, set the configuration option 'lineTerminator' to the character to use as the line terminator. When this data extractor is used, indicate the Label Items using zero-index position values of the fields.
  * **JSON** - UTF-8 encoded JSON data. This data can either reside in a JSON array on the event (for example [{object1},{object2},{object3}]) or can be a single object per 'line' (for example {object1}\n {object2}). To control the object delimiter, use the configuration option 'lineTerminator'.
  * **REGEX** - UTF-8 encoded strings of arbitrary data. With this configuration option, you must include the 'regularExpression' configuration option. This data extraction method also uses zero-indexed positional values for Label Items.
  * **OBJECT** - Serialized objects using Jackson JSON binary data. With this configuration option, you must include the 'class' configuration option. Using this data extraction method, an event can include only 1 serialized object.
* **dateItem** (String) - The attribute or field index that defines when the event occurred. This is used to generate the aggregate for the correct time period for the event. This can be formatted as a long value of epoch seconds, or a String value. If you provide a String value in the event, you must also set the configuration option 'dateFormat'. If it is omitted, then the timestamp of the event is set to the timestamp of the server instance when it processes the item.
* **dateAttributeAlias** (String) - Similar to labelAttributeAlias, this enables you to set the name of the date attribute in the aggregated data table.
* **dateFormat** (String) - The date format of the dateItem, using date format strings as specified at ```http://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html```.

You can also include the following options in the configuration:

* **summaryItems** (array&lt;String&gt;) - If the aggregator 'type' is SUM, then the aggregator automatically performs a time series aggregation on the summary items configured. These summary items must be numeric values that you want aggregated for the indicated time period and values of Label Items. For example, if your stream includes call data records, you might want to sum the duration of all calls made by mobile network by hour. Along with configuration of the Label Items and time, you would include a summary item of 'callDuration'. As with the configuration of the Label Items, summary items are zero-index positional values for CSV and regex data extractions, attribute names for JSON, and method names for OBJECT. For more information, see the Summary Items Mini-Language section.
* **filterRegex** (String) - Filters the stream data using a filtering regular expression. If provided, only the data that conforms to the regular expression is passed in for subsequent parsing. Note this step is applied on the raw underlying stream data as String values, and is not available for object serialization.
* **failOnDataExtraction** (boolean) - By default, the aggregator fails if it can't understand the data on the stream, to ensure that all events are properly accounted for. If you have a data stream that contains internally inconsistent data, and you want to perform a simple aggregation whenever you can successfully parse the data stream, set this value to 'true'. Alternatively, consider writing a **filterRegex** expression that extracts only the data that fits the configuration of Label, Date, and Summary Items.
* **tableName** (String) - Sets the name of the underlying time series data table in the data store.
* **environment** (String) - Runs an aggregator with a specified environment type. This enables you to separate the underlying data stores used for the time series data into production and test, for example.
* **readIOPS** (int) - Use with the default DynamoDB IDataStore to set up the number of read I/O operations per second (IOPS) you want on the time series data store.
* **writeIOPS** (int) - Use with the default DynamoDB IDataStore to set up how many write IOPS you want on the time series data store.
* **IDataStore** (String) - Configures alternative backing data stores other than DynamoDB. If you have written your own data store implementation, specify the full class name, including the package, to have this data store used. You can also specify the internal alternate data store 'com.amazonaws.services.kinesis.aggregators.datastore.DevNullDataStore', which does NOT store the time series data, and is useful only to consume the time series from CloudWatch.
* **emitMetrics** (boolean) - Emits the time series aggregated data as a custom CloudWatch domain of metrics. Set this value to 'true' to create a custom CloudWatch metric for the application name and namespace of the aggregator, with dimensions on the label and summary items.

### Summary Items Mini-Language

You can configure summary items and the type of summary using a miniature specification language, and navigate complex document structures in JSON data. You can apply the following type of summary transformations:

* **SUM** - Applies the default summary if you do not specify a summary type. This sums up all values seen for label and time values.
* **MIN** - Calculates the minimum value observed for the time period and label values.
* **MAX** - Calculates the maximum value observed for the time period and label values.
* **FIRST** - Stores the first observed value for the time period and label values.
* **LAST** - Is equal to the latest value for the time period and label values.

Summary items can have aliases applied, as in SQL, to control the name of the generated attribute in the data store you write to. You simply add the name of the item you require to the definition of the summary item, including functions.

You can also navigate an entity structure in a JSON-formatted stream data using dot notation; for example, given the following object, you can access the calculated duration using a summary item of 'timeValues.durations.calculated':

```
{
  "name": "Object To Be Aggregated",
  "timeValues": {
    "durations": {
      "calculated": 60,
      "recorded": 58
    },
    "endTime": "01/01/1970 01:00:00",
    "startTime": "01/01/1970 00:00:00"
  }
}
```

These concepts can be combined into a mini-specification:

Example 1 - Calculate the min, max, and sum of value 7 in a CSV stream, giving them friendly names - ```["min(7) min-purchase-price","max(7) max-purchase-price","sum(7) total-sales]"```

Example 2 - Calculate the sum and maximum value of the calculated duration in the JSON stream -  ```["sum(timeValues.durations.calculated)","max(timeValues.durations.calculated)"]```

### Sample Configurations

* **JSON** - http://amazon-kinesis-aggregators.s3.amazonaws.com/sample/json-aggregator.json
* **CSV** - http://amazon-kinesis-aggregators.s3.amazonaws.com/sample/csv-aggregator.json
* **Regular Expression** - http://amazon-kinesis-aggregators.s3.amazonaws.com/sample/regex-aggregator.json
* **Object Serialized Data** - http://amazon-kinesis-aggregators.s3.amazonaws.com/sample/object-aggregator.json

### Aggregator Data Structure

The data structure for aggregated data is arranged as a hash/range table in DynamoDB on the Label attributes and Date attribute at the configured granularity of time. Every table also includes the following:

* **eventCount** - The number of events consumed during the period.
* **lastWriteSeq** - The last sequence value from the Amazon Kinesis stream that generated an update to the time period and aggregate label.
* **lastWriteTime** - The time on the consumer application when the update was made to the aggregate data.
* **scatterPrefix** - A random number between 0 and 99 used to ensure that there are no write bottlenecks on global secondary indexes for the time period and last write sequence.

Of course, the table also includes any summary values that were added to the aggregator configuration. The format of these summary attributes in DynamoDB follow the pattern &lt;attribute&gt;-&lt;summary type&gt;, or use the alias provided.

* For JSON streams, the attribute is the attribute name configured.
* For object-serialized streams, the attribute is the summary method converted to a user-friendly name. For example 'getComputedValue' is written to the data store as 'computedValue'.
* For CSV and String data parsed using regular expressions, the attribute value is the position in the stream, indexed from 0.
* The summary type is one of the following values: MIN, MAX, SUM, FIRST, or LAST.

#### Indexes

All aggregator data stores have global secondary indexes (logically) on the date value and on lastWriteSeq. To ensure adequate write performance, these indexes are structured as hash/range on the scatterPrefix (a random number between 0 and 99) and the value is indexed.

#### Web-based Query API

The Amazon Kinesis Aggregators web application also provides several query API operations, which return data in the JSON format. When deployed, you can make an HTTP request to a variety of endpoints to retrieve different types of data. Currently, there is no security offered for the Web API operations, so you must ensure that they are only accessible from within your VPC using security group rules or similar. Do NOT make these endpoints publicly accessible.

##### Viewing the Running Configuration

You can view the configuration of your aggregators at the URL ```<web application>/configuration```, which returns an object such as:

```
{
  "application-name": "EnergyRealTimeDataConsumer",
  "config-file-url": "s3://mybucket/kinesis/sensor-consumer-regex.json",
  "environment": null,
  "failures-tolerated": null,
  "max-records": "2500",
  "position-in-stream": "LATEST",
  "region": "eu-west-1",
  "stream-name": "EnergyPipelineSensors",
  "version": ".9.2.7.4"
}
```

##### Date-based Queries

Use the Date query to find data that has been aggregated on the basis of the stream timestamp value. For example, use this interface to periodically retrieve all new data that has been processed, or to pull data for specific time ranges for comparative analysis. The URL is:

```
<web application>/dateQuery?params
```

Parameters:

* **namespace** - The namespace for the aggregator configuration.
* **operator** - The condition to query for, from the DynamoDB ComparisonOperator enum: EQ, GT, GE, and so on. Note that BETWEEN is not yet supported.
* **granularity** – The granularity of time required, from the TimeHorizon enum: SECOND, MINUTE, HOUR, and so on.
* **date–value** – The date value to query relative to, in yyyy-MM-dd+hh:mm:ss format (for example, 2014–09–01+18:00:00).

This returns all data from the aggregated table for the date period specified.

You can also use the internal Java API:

```
public List<Map<String, AttributeValue>> queryByDate(Date dateValue, TimeHorizon h,
ComparisonOperator comp, int threads) throws Exception
```

This method queries by the Date, TimeHorizon, and ComparisonOperator values you select. For example, to find all hourly aggregates after 3pm, use:

```
dateValue=Date('2014-01-01 15:00:00'), TimeHorizon.HOUR, ComparisonOperator.GT
```

The Threads parameter is the number of threads used to do the query. This is due to the index being organized on hash/range of scatterPrefix/DateValue.

##### Query for Label/Date Values

To query the application to find the unique set of labels and date values that have been aggregated, use the following URL:

```<web application>/keyQuery?params```

Parameters:

* **namespace** - The namespace for the aggregator configuration.
* **scope** - Use 'HashKey' to get just the unique aggregate label values or 'HashAndRangeKey' to get both the label and date values.

This returns a unique list of all keys from the aggregated table.

You can also use the internal Java API:

```
public Map<String, AttributeValue> queryValue(String label, Date dateValue, TimeHorizon h)
throws Exception
```

This method takes the label you are interested in, as well as a date for the date value. If you have multiple TimeHorizon values configured on the aggregator, it generates the correct dateValue to query the underlying table with. You are likely to use this interface to query across aggregator data stores looking for related time-based values.

## Integrating Aggregators into Existing Java Applications

In addition to running aggregators as stand-alone Amazon Kinesis applications, you can integrate them into existing Amazon Kinesis applications. You can:

* Run the managed consumer from an existing control environment
* Inject a set of aggregators into a managed IRecordProcessorFactory
* Use an existing IRecordProcessor to send data to one or more aggregators

### Managed IRecordProcessorFactory

To build your Amazon Kinesis worker and configure it explicitly, you can still use aggregators to create IRecordProcessorFactory. In this case, simply create a new instance of com.amazonaws.services.kinesis.aggregators.processor.AggregatorProcessorFactory with the configured aggregators.

### Integration with Existing IRecordProcessors

If you have an existing worker application and you simply want to add the aggregation capability, you can directly integrate with one or more aggregators. To do this, simply construct the aggregators using a configuration file, or using a pure Java configuration. Then, to inject new data into the aggregator, simply call:

```void aggregate(List<record> records)```

This causes the time series calculations to be done based upon the configuration of the aggregators. Then, when your worker normally calls checkpoint(), also call:

```void checkpoint()```

This flushes the in-memory time series state to the backing data store. You must ensure that the aggregators are initialized correctly against the shard for the worker by calling this method in the existing KCL Application IRecordProcessor initialize() method:

```void initialize(String shardId)```

You must also ensure that if the shutdown() method is invoked on your Amazon Kinesis application, you call:

```void shutdown(boolean flushState)```
If the shutdown reason specified in the shutdown method for IRecordProcessor is ShutdownReason.ZOMBIE, set flushState to 'false' to allow the data to be re-aggregated by another worker. However, if the value is ShutdownReason.TERMINATE, you should flush the aggregator state on termination.

### Configuring Aggregators in Existing Applications

There are a variety of ways to configure aggregators when you are integrating into existing applications. You might use a factory to create one or more aggregators from a simple set of arguments, or you can configure each aggregator directly and manage it as part of an aggregator group.

#### Aggregator Factories

There are a variety of aggregator factories available in the com.amazonaws.services.kinesis.aggregators.factory package, which generally map to the configuration types found in the configuration file. In fact, you can use configuration files to configure aggregators from Java using the following:

```
ExternallyConfiguredAggregatorFactory.buildFromConfig(  
String streamName,  
String applicationName,  
KinesisClientLibConfiguration config,  
String configFile)  
```  
You can also take advantage of aggregators that are specific to the type of data to be aggregated:

##### JSON Data

```
JsonAggregatorFactory.newInstance(String streamName  
, String appName  
, KinesisClientLibConfiguration config  
, String namespace  
, TimeHorizon timeHorizon  
, AggregatorType aggregatorType  
, List<string> labelAttributes  
, String dateAttribute  
, String dateFormat 
, List<string> summaryAttributes)  
```
##### CSV Data

```
CsvAggregatorFactory.newInstance(String streamName  
, String appName  
, KinesisClientLibConfiguration config  
, String namespace  
, TimeHorizon timeHorizon  
, AggregatorType aggregatorType  
, String delimiter  
, List<integer> labelIndicies  
, int dateIndex  
, String dateFormat 
, List<object> summaryIndicies)  
```
##### String Data parsed with Regular Expressions

```
RegexAggregatorFactory.newInstance(String streamName  
, String appName  
, KinesisClientLibConfiguration config  
, String namespace  
, List<timehorizon> timeHorizons  
, AggregatorType aggregatorType  
, String regularExpression  
, List<integer> labelIndicies  
, int dateIndex  
, String dateFormat  
, List<object> summaryIndicies)  
```
##### Object Serialized Data

You can generate aggregators for object-serialized data using annotations:

```
ObjectAggregatorFactory.newInstance(String streamName  
, String appName  
, KinesisClientLibConfiguration config  
, Class clazz)  
```
Note that 'clazz' is a class that has been configured using annotations found in the com.amazonaws.services.kinesis.aggregators.annotations package. This factory method throws an error if the class is not annotated.

Alternatively, you can configure the aggregator directly:

```
ObjectAggregatorFactory.newInstance(String streamName  
, String appName  
, KinesisClientLibConfiguration config  
, String namespace  
, List<TimeHorizon> timeHorizons  
, AggregatorType aggregatorType  
, Class clazz  
, List<String> labelMethods  
, String dateMethod  
, List<String> summaryMethods)
```

#### Direct Configuration

If you want even more control over the configuration of a given set of aggregators, then you can configure them directly. To effectively do this, you must understand how aggregators work. Aggregators are built around several subsystems that their factory methods configure automatically. When you build aggregators directly, you must construct an aggregator from its constituent subsystems. For more information, see the 'Extending Aggregators' section of this document.

To configure an aggregator directly, you must configure two of the subsystems: the aggregator and the IDataExtractor that extracts the data from the stream.

##### IDataExtractor

When you create an aggregator directly, you must specify the IDataExtractor to get data out of the stream for aggregation. There are IDataExtractors in the com.amazonaws.services.kinesis.aggregators.io package. Each of these map to the supported data formats, and provide relevant configuration options, including label, Date, and summary items. IDataExtractors use fluent builders for all optional configurations. For example, creating a JsonDataExtractor looks like this:

```
new JsonDataExtractor(labelAttributes)  
.withDateValueAttribute(dateAttribute)  
.withSummaryAttributes(summaryAttributes)  
.withDateFormat(dateFormat);  
```

##### Aggregator

You then create the aggregator with the options that are specific to it, including KinesisClientLibConfiguration, required TimeHorizon values, and options for emitting metrics. For example, using the example JsonDataExtractor, you might configure the aggregator as follows:

```
return new StreamAggregator(streamName, appName, namespace, config, dataExtractor)  
.withTimeHorizon(timeHorizons)  
.withAggregatorType(aggregatorType)  
.withCloudWatchMetrics(true);  
```

## Extending Aggregators

You might want to extend aggregators for a variety of reasons. The use cases that we know of today that will require extension include supporting data on a stream that is compressed, encrypted, and uses an object serialization format other than Jackson/JSON, or implementing large objects. We designed aggregators with extensibility in mind. You can extend the framework at the following integration points.

### Data Format & Handling

The ability to support CSV, JSON, arbitrary string data and object serialization is provided by the IDataExtractor and IKinesisSerializer interfaces, residing at com.amazonaws.services.kinesis.aggregators.io and io.Serializer.

#### IKinesisSerializer

This interface interoperates between the internal data format used by IDataExtractors, and byte arrays are used on the stream. You implement IKinesisSerializer to support compressed stream data or if your data is encrypted, for example. The implementation would conform to the following interface, which is identical to the Amazon Kinesis Connector ITransformer class:

```
/**  
* Transforms data from a Record (byte array) to the data  
* model class (T) for processing in the application and from the data model  
* class to the output type (U) for the emitter.  
* 
* @param <T> the data type stored in the record  
*/
public interface IKinesisSerializer<T, U> {  
/**
* Transform the record into an object of its original class.  
* 
* @param record raw record from the stream  
* @return data using its original class  
* @throws IOException if it could not convert the record to a T  
*/
public T toClass(InputEvent event) throws IOException;  

/**
* Transform the record from its original class to a byte array.  
* 
* @param record data as its original class  
* @return a data byte array  
*/
public U fromClass(T record) throws IOException;  
}  
```
#### IDataExtractor

IDataExtractors take the deserialized data and extract the relevant Label, Date, and Summary items. They also typically do any filtering that is exposed by the IDataExtractor. Implement a new IDataExtractor if the type of data returned by a custom IKinesisSerializer implementation is not compatible with the existing IDataExtractors in the io package. This new IDataExtractor would conform to:

```
/**
* Enables pluggable data extractors for different types of
* stream data. Aggregators use IDataExtractor to interoperate between the
* stream data format and the internal format required for aggregation.
* IDataExtractors likely use IKinesisSerializers to read and write to and from
* the stream
*/
public interface IDataExtractor {  
/**  
* Gets the name of the label value to be extracted.  
*   
* @return  
*/  
public String getAggregateLabelName();  

/**  
* Gets the name of the date value to be extracted.  
*   
* @return  
*/  
public String getDateValueName();  

/**
* Extracts one or more aggregatable items from a Amazon Kinesis record.  
*  
* @param event The Amazon Kinesis record from which we want to extract data.  
* @return A list of ExtractedData elements that have been resolved from  
*         the input data.  
* @throws SerializationException  
*/
public List<AggregateData> getData(InputEvent event) throws SerialisationException;

/**
* Sets the type of aggregator that contains this IDataExtractor. Used to
* boost efficiency in that the extractor will not extract summary items for
* COUNT-based aggregator integration.
* 
* @param type
*/
public void setAggregatorType(AggregatorType type);

/**
* Validates that the extractor is well formed.
* 
* @throws Exception
*/
public void validate() throws Exception;

/**
* Gets the summary configuration that is driving data extraction against the
* data stream.
* 
* @return
*/
public SummaryConfiguration getSummaryConfig();

public IDataExtractor copy() throws Exception;
}
```

Also note that an IDataExtractor returns multiple aggregatable objects from the stream. If you had a requirement to support M:N Kinesis Events to Aggregatable Events, an IDataExtractor could do the job using local state.

Note that the IDataExtractor is STATEFUL for the life of an aggregator running on a shard, and contains the configuration of the data that is to be extracted. Because a new IDataExtractor is generated when a new aggregator is initialized on a shard, you must ensure that it is thread-safe and implement the copy() interface correctly to ensure that multiple instances can operate within a single JVM.

### Data Store

The Amazon Kinesis Aggregators framework backs its data onto DynamoDB, and takes advantage of powerful DynamoDB features such as hash/range keys, atomic increment, and conditional updates. It also implements a defensive flush mechanism, which means that at any provisioned I/O rate, the aggregator can flush its state to DynamoDB without timing out.

To extend aggregators with support for an alternate backing store, such as a relational database or Redis, implement com.amazonaws.services.kinesis.aggregators.datastore.IDataStore. This implementation must meet the following service levels:

* Flushes all internal state to the data store in 5 minutes or less (this is due to the Amazon Kinesis worker timeout)
* Supports a composite primary key for all label values and date value
* Performs an atomic, transactional increment operation
* Conditionally updates a discrete value in the table

The implementation of a new IDataStore must conform to the following:

```
/**
* Enables the in-memory cached aggregates 
* to be saved to a persistent store
*/
public interface IDataStore {
/**
* Writes a set of Update key/value pairs to the backing store
* 
* @param data The input dataset to be updated
* @return A data structure that maps a set of
*         AggregateAttributeModifications to the values that were
*         affected on the underlying data store, by UpdateKey
* @throws Exception
*/
public Map<UpdateKey, Map<String, AggregateAttributeModification>> write(
Map<UpdateKey, UpdateValue> data) throws Exception;

/**
* Method called on creation of the IDataStore
* 
* @throws Exception
*/
public void initialize() throws Exception;

/**
* Method that is periodically invoked to allow the IDataStore to
* refresh tolerated limits for how often write() should be called
* 
* @return
* @throws Exception
*/
public long refreshForceCheckpointThresholds() throws Exception;

/**
* Sets the region for the IDataStore
* 
* @param region
*/
public void setRegion(Region region);
}
```

### Metrics Service

By default, Amazon Kinesis Aggregators integrates with CloudWatch for the purpose of metrics dashboards and alerts. However, you might want to push metrics to platforms such as Ganglia or New Relic. In these cases, you would provide an implementation of the com.amazonaws.services.kinesis.aggregators.metrics.IMetricsEmitter. This implementation would conform to the following:

```
/**
* Provides classes that can write to metrics services. 
* Receives the output of the IDataStore modifications, and applies the data to
* the metrics service.
*/
public interface IMetricsEmitter {
/**
* Emits a new set of metrics to the metrics service
* 
* @param metricData Input Data to be intrumented
* @throws Exception
*/
public void emit(Map<UpdateKey, Map<String, AggregateAttributeModification>> metricData)
throws Exception;

/**
* Sets the region of the metrics service
* 
* @param region
*/
public void setRegion(Region region);
}
```

Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

http://aws.amazon.com/asl/
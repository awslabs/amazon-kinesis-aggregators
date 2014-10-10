# Amazon Kinesis Aggregators

Amazon Kinesis Aggregators is a Java framework which allows the automatic creation of real time aggregated time series data from Streams on Amazon Kinesis. This dataset can answer questions such as ‘how many times per second has ‘x’ occurred’ or ‘what was the breakdown by hour over the day of the streamed data containing ‘y'. Using this framework, you simply describe the format of the data on your stream (CSV, JSON, etc), describe what granularity of times series you require (Seconds, Minute, Hour, etc) and how the data elements which are streamed should be grouped, and the framework handles all the time series calculations and data persistence. You then simply consume the time series aggregates in your application via Amazon Dynamo DB, or interact with the time series using Amazon CloudWatch, or through a Web Query API. You can also analyse the data using Hive on Amazon Elastic MapReduce, or bulk import it to AmazonRedshift. The process runs as a standalone Kinesis Enabled Application, or can be integrated into existing Kinesis applications.

The data is stored in a time series on the basis of how you aggregate it. An example of a dataset aggregating Telecoms Call Data Records in Amazon Dynamo DB might look like:

http://meyersi-ire-aws.s3.amazonaws.com/KinesisDynamicAggregators/img/Screen%20Shot%202014-05-27%20at%2011.53.21.png

The corresponding data in CloudWatch would look like:

https://s3-eu-west-1.amazonaws.com/meyersi-ire-aws/KinesisDynamicAggregators/img/Screen+Shot+2014-05-29+at+08.21.57.png

## Building Aggregators

Amazon Kinesis Aggregators is built with Apache Maven. To build, simply run Maven from the amazon-kinesis-aggregators directory. You will find the following build artefacts are created in the 'target' directory:

* **amazon-kinesis-aggregators-<version>.jar** - this is the Kinesis Aggregators Application, without compiled dependencies
* **AmazonKinesisAggregators.jar-complete.jar** - this is the Kinesis Aggregators Application with all required dependencies included
* **AmazonKinesisAggregator.war** - this is the Web Application Archive for deployment to Java Application Servers

## Running Aggregators

Amazon Kinesis Aggregators ships with several deployment options which should enable you run with a minimum of operational overhead, while also accommodating advanced deployment use cases. You can run Kinesis Aggregators as:

* A fully managed Elastic Beanstalk Application. All you must do is deploy the KinesisAggregators.war Web Archive, and provide a configuration file that is accessible via HTTP.
* A managed Java client, running through host orchestration your choosing. For example, you might deploy this managed Java client as part of an EC2 Fleet that uses Autoscaling.
* As part of an existing Kinesis Enabled Application. This allows an existing KCL to 'sideload' Aggregator processing, as an augmentation to an already established Kinesis application.

### Running Aggregators with Elastic Beanstalk

Amazon Kinesis Aggregators compiles a Web Application Archive (WAR) which allows for easy deployment on Java Application Servers such as Apache Tomcat on Amazon Elastic Beanstalk (http://aws.amazon.com/elasticbeanstalk). Kinesis Aggregators also includes configuration options which inform Elastic Beanstalk to scale the Application on CPU load, which is typically the bottleneck for Kinesis Applications as they scale up. This is the recommended deployment method for Kinesis Aggregators.

To deploy Kinesis Aggregators as an Elastic Beanstalk Application, Create a new Elastic Beanstalk Application, and when prompted by the AWS Console, upload the KinesisAggregators.war file from your local build. Select an instance type that will be suitable for the type of Aggregation you are running (specifically, the higher the granularity of label items, and the more fine grained the TimeHorizon, the larger of an instance you will require). Once deployed, click the URL for the Application Environment, and you will see message:

```OK - Kinesis Aggregators Managed Application hosted in Elastic Beanstalk Online ```

furthermore, if you snapshot the logs, you will see a log line indicating:

```No Aggregators Configuration File found in Beanstalk Configuration config-file-url. Application is Idle```

This indicates that the application is deployed, but not configured. To configure, add Elastic Beanstalk configuration parameters as required: 

* **stream-name** - The name of the Stream to consume
* **application-name** - The name to use for the KCL application
* **failures-tolerated** - The number of Worker exceptions allowed before the Worker terminates
* **position-in-stream** - The position in the Stream to start consuming data from. Either 'LATEST' or 'TRIM_HORIZON'.
* **max-records** - Maximum number of records to consume from Amazon Kinesis in a single cycle. You may wish to set this value if your Stream processing (in addition to aggregation) can be slow
* **region** - The region to use for the entire processing application. This includes the Amazon Kinesis Stream, the Amazon Dynamo DB lease tables, as well as the Amazon CloudWatch and Aggregate data stores. Kinesis Aggretors does not currently support cross-Region deployment
* **environment** - An environment name to set on the application. This will ensure that all tables in Amazon Dynamo DB are prefixed with the environment, allowing you to keep datasets separate for Test vs Production (for example)
* **config-file-url** - The URL for the configuration file.

Once added, click 'Save' and Elastic Beanstalk will apply the changes to the environment. Once done, wait a minute or so, and then Snapshot Logs to confirm that the Aggregators configured are running.

### Running the managed Java client application

This is a great option if you have data on Amazon Kinesis, but have an existing way you want to run the Java client other than Elastic Beanstalk. You can start the application from a server using:
```java -cp AmazonKinesisAggregators.jar-complete.jar -Dconfig-file-path=<configuration> -Dstream-name=<stream name> -Dapplication-name=<application name> -Dregion=<region name - us-east-1, eu-west-1, etc> com.amazonaws.services.kinesis.aggregators.consumer.AggregatorConsumer```. In addition to the configuration items as outlined in the Elastic Beanstalk section, you use configuration item:

* **config-file-path** - The path to the configuration file

to configure the config file. It is recommended that you run your servers with an Autoscaling Group to ensure tolerance of host failures.

### Configuration

The configuration file is able to create one or more aggregations against the same stream. It is a json file which creates a set of aggregator objects, all of which will be managed by the Framework. You would create one aggregator for each distinct Label that you want to aggregate on. Each Aggregator can then have it's own properties of Time granularity, Aggregator Type, and so on.

The core structure of the config file is an array of Aggregator objects. For example, a configuration which created two aggregators would contain:
```[{aggregatorDef1}, {aggregatorDef2}]```
where aggregatorDefN is an Aggregator configuration. An Aggregator configuration must include the following attributes:

* **namespace** (String) - The namespace allows you to create separate time series data stores from each other. This namespace will be used with the application name and environment to create the underlying data tables for the time series, as well as the namespace for custom CloudWatch metrics. Use something that's meaningful based upon the Label and time granularity
* **labelItems** (array<String) - The Label Items include a list of the elements of the data stream to aggregate on. The data stored in the time series will be aggregated by the unique values from the stream for these attributes, and by time. For instance, if we wanted to aggregate data for searches made against a Car website, we might have a label item set of ["Make","Model","Year"]. If we are using CSV data, then this same configuration might be positional based on the fields in the line, such as [0,3,5]
* **labelAttributeAlias** (String) - The label attribute alias allows you to give the target database attribute for the label items a name. This is particularly useful where you are using CSV or Regex extracted data, and otherwise would end up with a label attribute named the same as the label attribute index.
* **type** (enum) - The type of the Aggregation to run. Available types are 'COUNT' and 'SUM'. Counting aggregators simply count the instances of unique values in the Label Items by time. Using our previous example, it would generate a count of searches by configured time period for each unique combination of Make, Model and Year. Building on this 'SUM' type Aggregators also will calculate summaries of other numeric values on the Stream. Please see the configuration option **'summaryItems**' for more information.
* **timeHorizons** (array&lt;enum&gt;) - Because the data is captured as a time series, you must tell the Aggregator what definition of time you require. To have the data on the Stream aggregated by Minute, you would indicate 'MINUTE'. To put data into buckets of 5 minutes duration, you'd use MINUTES_GROUPED(5). You may indicate multiple timeHorizon values, and the Aggregator will automatically maintain the time series data at that granularity. A very common configuration might be ["SECOND","HOUR","FOREVER"] which would give us per second aggregates, a rollup by Hour, and then a simple dataset to view everything that has ever occurred in a single values. Available values are:
  * SECOND
  * MINUTE
  * MINUTES\_GROUPED(int minutePeriod) - data will be grouped into time buckets using the minute period length indicated. To have 4 buckets per hour, use '15', or use value '5' for 5 minute long buckets of aggregation
  * HOUR
  * DAY
  * MONTH
  * YEAR
  * FOREVER - this time value will roll up everything seen in a single value '*'. This can be a very useful metric to simply know how often something has ever occurred
* **dataExtractor** (enum) - The data extractor entry tells the Aggregator how to parse and extract the Label Items from your Stream. Currently the following data formats are supported for external configurations using the configFile:
  * **CSV** - data on the stream is formatted as character separated UTF-8 data. The default delimiter is comma. If you want to override the delimiter, then set Aggregator configuration option 'delimiter' to the character value which is used as the field terminator. Also please note that all data extractors support multi-value events. This means that you can have many CSV 'lines' within a single event, which will be extracted with a line terminator of "\n". If you want to override the line terminator on any data extractor which is text based, then you may set Aggregator configuration option 'lineTerminator' to the character used as the line terminator. When this data extractor is used, Label Items should be indicated using zero index position values of the fields.
  * **JSON** - data on the stream is formatted as UTF-8 encoded JSON data. This data can either reside in a JSON array on the event - for example [{object1},{object2},{object3}] - or can be a single json object per 'line' - for example {object1}\n {object2}. To control the object delimiter, use configuration option 'lineTerminator'
  * **REGEX** - data on the stream is UTF-8 encoded strings of arbitrary data. With this configuration option you must include the 'regularExpression' configuration option. This data extraction method also uses zero indexed positional values for Label Items.
  * **OBJECT** - data on the stream is binary serialised objects using Jackson JSON binary data. With this configuration option you must include the 'class' configuration option as well. Using this data extraction method, an event may only include 1 serialised object
* **dateItem** (String) - the date Item is the attribute or field index to be used for the definition of when the event occurred. This will be used to generate the aggregate for the correct time period for the event. This can be formatted as a long value of epoch seconds, or instead can be a String value. If a String value is supplied in the event, then you must also set configuration option 'dateFormat'. If omitted, then the timestamp of the Event will be set to the timestamp of the Server Instance when it processes the item.
* **dateAttributeAlias** (String) - Similar to labelAttributeAlias, this allows you to set the name of the date attribute in the aggregated data table
* **dateFormat** (String) - the date format to be used to understand the value of the dateItem using date format strings as specified at http://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html

You may also include the following options in an Aggregator configuration:

* **summaryItems** (array&lt;String&gt;) - if the Aggregator 'type' is SUM, then the Aggregator will automatically perform a time series aggregation on the Summary Items configured. These Summary Items must be numeric values that you want to have aggregated for the indicated time period and values of Label Items. For example, if our Stream included Call Data Records, we might want to sum up the duration of all calls made by mobile network by hour. Along with configuration of the Label Items and time, we would include a summary item of 'callDuration'. As with the configuration of the Label Items, Summary Items will be zero index positional values for CSV and REGEX data extractions, attribute names for JSON, and method names for OBJECT. Please see the Summary Items mini language for more details on how to configure Summary Items.
* **filterRegex** (String) - You can filter the Stream data by indicating a filtering regular expression. If provided, only data which conforms to the regular expression will be passed in for subsequent parsing. Please note this step is applied on the raw underlying Stream data as String values, and is not available for Object serialisation.
* **failOnDataExtraction** (boolean) - by default, the Aggregator will refuse to proceed if it can't understand the data on the Stream, to ensure that all events are properly accounted for. If you have a data stream which contains data which is internally inconsistent, and you want to simple Aggregate whenever you can successfully parse the data stream, then set this value to 'true'. Alternatively, consider writing a **filterRegex** which will extract only the data which fits the configuration of Label, Date and Summary Items.
* **tableName** (String) - Use this configuration option to set the name of the underlying time series data table in the data store
* **environment** (String) - Use the configuration option to run an Aggregator with a specified environment type. This will allow you to separate the underlying data stores used for the time series data into Production vs Test, for example.
* **readIOPS** (int) - Use this configuration option with the default Dynamo DB IDataStore to set up how many Read IO's Per Second you want on the time series data store
* **writeIOPS** (int) - Used with the default Dynamo DB IDataStore to set up how many Write IO's Per Second you want on the time series data store
* **IDataStore** (String) - Allows for the configuration of alternative backing data stores other than Dynamo DB. If you have written your own data store implementation, then indicate the full class name including package to have this data store used. You can also indicate the internal alternate data store 'com.amazonaws.services.kinesis.aggregators.datastore.DevNullDataStore' which will NOT store the time series data, which is useful if you only want to consume the time series from CloudWatch.
* **emitMetrics** (boolean) - Emit the time series aggregated data as a custom CloudWatch domain of metrics. Setting this value to 'true' will create a new custom CloudWatch metric for the Application Name and Namespace of the Aggregator, with Dimensions on the Label Items and Summary Items

### Summary Items Mini Language

Summary items can be configured using a miniature specification language to change the type of summary, as well as navigate complex document structures in JSON data. You can apply the following type of summary transformations:

* **SUM** - the default summary that will be applied if you do not indicate a summary type. This will sum up all values seen for Label and time values
* **MIN** - this summary will calculate the minimum value observed for the time period and label values
* **MAX** - this summary will calculate the maximum value observed for the time period and label values
* **FIRST** - this summary will only store the first observed value for the time period and label values
* **MAX** - this summary will always be equal to the latest value for the time period and label values

Summary items can have aliases applied like in SQL to control the name of the generated attribute in the data store you write to. You simply add the name of the item you require to the definition of the summary item including functions.

You can also navigate an entity structure in JSON formatted stream data using dot notation. For example, given object:
```{"name":"Object To Be Aggregated",
 "timeValues": {
   "startTime":"01/01/1970 00:00:00",
   "endTime":"01/01/1970 01:00:00",
   "durations":{
     "recorded":58,
     "calculated":60
   }
  }
}```

We can access the calculated duration in the above example using a summaryItem of 'timeValues.durations.calculated'. These concepts can be combined into a mini specification:

Example 1 - calculate the min, max and sum of value 7 in a CSV stream, giving them friendly names - ["min(7) min-purchase-price","max(7) max-purchase-price","sum(7) total-sales"

Example 2 - calculate the sum and maximum value of the calculated duration in our JSON stream - ["sum(timeValues.durations.calculated)","max(timeValues.durations.calculated)"]

### Sample Configurations

* **JSON** - https://s3-eu-west-1.amazonaws.com/meyersi-ire-aws/KinesisDynamicAggregators/sample/json-aggregator.json
* **CSV** - https://s3-eu-west-1.amazonaws.com/meyersi-ire-aws/KinesisDynamicAggregators/sample/csv-aggregator.json
* **Regular Expression** - https://s3-eu-west-1.amazonaws.com/meyersi-ire-aws/KinesisDynamicAggregators/sample/regex-aggregator.json
* **Object Serialised Data** - https://s3-eu-west-1.amazonaws.com/meyersi-ire-aws/KinesisDynamicAggregators/sample/object-aggregator.json

### Aggregator Data Structure

The data structure for Aggregated Data is arranged as a Hash/Range table in Dynamo DB on the Label Attributes and Date Attribute at the configured granularity of time. Every table also includes:

* **eventCount** - the number of events consumed during the period
* **lastWriteSeq** - the last Sequence value from the Kinesis stream which generated an update to the time period and aggregate label
* **lastWriteTime** - the time on the consumer application when the update was made to the aggregate data
* **scatterPrefix** - this value is a random number between 0 and 99 used for ensuring that there are no write bottlenecks on Global Secondary Indexes for the time period and last write sequence

Of course the table will also include any summary values that were added to the Aggregator configuration. The format of these summary attributes in Dynamo DB will follow pattern &lt;attribute&gt;-&lt;summary type&gt;, or use the Alias supplied.

* For JSON Streams, the attribute will be the attribute name configured
* For Object Serialised Data Streams, the attribute will be the summary method converted to a more user friendly name - for example 'getComputedValue' will be written to the datastore as 'computedValue'
* For CSV and String data parsed with Regular Expressions, the attribute value will be the position in the stream indexed from 0
* Summary Type is one of MIN, MAX, SUM, FIRST or LAST

#### Indexes

All Aggregator data stores have global secondary indexes (logically) on the date value and on lastWriteSeq. To ensure adequate write performance, these indexes are structured as Hash/Range on the scatterPrefix (a random number between 0 and 99) and the value indexed.

### Querying the Data

The StreamAggregator class provides several interfaces for querying the underlying data. The first is used to query for a specific data value from the aggregate table. The second allows for querying based upon the date range with conditional logic.

#### Querying for a specific aggregate

You would use the following interface to query for a specific label value and time period.
```public Map<String, AttributeValue> queryValue(String label, Date dateValue, TimeHorizon h)
            throws Exception
```
This method takes the label you are interested in, as well as a Date for the date value. Based on if you have multiple TimeHorizons configured on the Aggregator, it will generate the correct dateValue to query the underlying table with. It is likely that you would use this interface to query across Aggregator data stores looking for related time based values.

#### Querying for Data by Date

Perhaps more commonly, you will want to query for data by Date range, based upon the date in the Stream. To do this, use method:
```public List<Map<String, AttributeValue>> queryByDate(Date dateValue, TimeHorizon h,
            ComparisonOperator comp, int threads) throws Exception
```
This method queries on the basis of the Date Value supplied, for the TimeHorizon you are interested in, and with the ComparisonOperator you select. For instance, to find all Hourly Aggregates from 3pm forward, you'd use:
```dateValue=Date('2014-01-01 15:00:00'), TimeHorizon.HOUR, ComparisonOperator.GT
```
The Threads parameter is the number of Threads used to do the query. This is due to the index being organised on Hash/Range of scatterPrefix/DateValue.

##### Web Based Query API
The Aggregators Web Application also provides several query API's which return data in JSON format. When deployed, you can make an HTTP request to a variety of endpoint to retrieve different types of data.

###### Date Based Queries

You would use the Date Query to find data that has been Aggregated on the basis of the Stream timestamp value. For example, you would use this interface to periodically retrieve all new data that has been processed, or to pull data for specific time ranges for comparitive analysis. The URL is:

```
<web application>/dateQuery?params
```

Parameters:

* **namespace** - The namespace for the Aggregator configuration to be queried
* **operator** - The condition to query for, from the Dynamo ComparisonOperator enum - EQ, GT, GE, etc. BETWEEN is not yet supported
* **granularity** – The granularity of time required, from TimeHorizon enum – SECOND, MINUTE, HOUR etc
* **date–value** – The date value to query relative to, in yyyy-MM-dd+hh:mm:ss format (e.g. 2014–09–01+18:00:00)

This returns all data from the aggregated table for the date period specified

###### Query for Label/Date Values

You may also want to query the Application just to find the unique set of Labels and Date Values that have been aggregated. To do this, use URL:

```
<web application>/keyQuery?params
```

Parameters:

* **namespace** - The namespace for the Aggregator configuration to be queried
* **scope** - Either 'HashKey' to just get the unique aggregate label values or 'HashAndRangeKey' to get both the label and date values

This returns a unique list of all keys from the aggregated table.

Currently there is no security offered for the Web API's, so you must ensure they are only accessible from within your VPC via security group grants or similar. Do NOT make these endpoints publicly accessible.

## Integrating Aggregators Into Existing Applications

In addition to running Aggregators as a standalone managed KCL application, you can integrate them into an existing KCL application. You can:

* Run the managed consumer from an existing control environment
* Inject a set of Aggregators into a managed IRecordProcessorFactory
* Use an existing IRecordProcessor and explicitly send data to one or more Aggregators

### Managed IRecordProcessorFactory

If you want to build your Kinesis Application Worker and configure it explicitly, you can still use Aggregators to create the IRecordProcessorFactory. In this case simply create a new instance of the com.amazonaws.services.kinesis.aggregators.processor.AggregatorProcessorFactory with the configured Aggregators that should be run.

### Integration with existing IRecordProcessors

If you have an existing Worker application, and you simply want to add Aggregation capability into it, then you can directly integrate with one or more Aggregators. To do this, simply construct the Aggregators using a configuration file, or with a pure Java configuration. Then to inject new data into the Aggregator, simply call:

```void aggregate(List<record> records)```

This will cause the time series calculations to be done based upon the configuration of the Aggregators. Then, when your worker normally calls checkpoint(). Also call:

```void checkpoint()```

which will cause the in memory time series state to be flushed to the backing data store. You must also ensure that the Aggregators are initialised correctly against the Shard for the worker by calling

```void initialise(String shardId)```

in the existing KCL Application IRecordProcessor initialise() method, and also ensure that if the shutdown() method is invoked on your existing KCL application, that you call:

```void shutdown(boolean flushState)```

If the shutdown reason supplied to the shutdown method for the IRecordProcessor is ShutdownReason.ZOMBIE then you should supply boolean value 'false' to the flushState argument to allow the data to be reaggregated by another worker. However if the value is ShutdownReason.TERMINATE then you should flush the Aggregator state on termination.

### Configuring Aggregators in Existing Applications

There are a variety of ways to configure Aggregators when you are integrating into existing applications. You might use a Factory to create one or more Aggregators from a simple set of arguments, or you can configure each Aggregator directly and manage it as part of an AggregatorGroup.

#### Aggregator Factories

There are a variety of Aggregator Factories available in the com.amazonaws.services.kinesis.aggregators.factory package, which generally map to the configuration types found in the config file. In fact, you can use config files to configure Aggregators from java by utilising:

```
ExternallyConfiguredAggregatorFactory.buildFromConfig(  
String streamName,  
String applicationName,  
KinesisClientLibConfiguration config,  
String configFile)  
```  
You can also take advantage of Aggregators which are specific to the type of data to be aggregated:

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
##### Object Serialised Data

You can generate Aggregators for object serialised data using Annotations:

```
ObjectAggregatorFactory.newInstance(String streamName  
, String appName  
, KinesisClientLibConfiguration config  
, Class clazz)  
```
where 'clazz' is a class that has been configured using Annotations found in the com.amazonaws.services.kinesis.aggregators.annotations package. This factory method will throw an error if the class is not annotated.

Or alternatively you can configure the Aggregator Directly:

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

If you want even more control over the configuration of a given set of Aggregators, then you can configure them directly. To effectively do this, you need to understand a little bit about how Aggregators work. Aggregators are built around several subsystems, that they Factory methods configure automatically. When we build Aggregators directly we must construct an Aggregator from its constituent subsystems. Please see the 'Extending Aggregators' section of this documentation for more information on what these subsystems are and how they work.

To configure an Aggregator directly, we need to only configure 2 of the subsystems: the Aggregator, and the IDataExtractor that extracts the data from the Stream.

##### IDataExtractor

When you create an Aggregator directly, you must supply the IDataExtractor to be used to get data out of the Stream for Aggregation. There are IDataExtractors supplied in the com.amazonaws.services.kinesis.aggregators.io package. Each of these map to the supported data formats, and provide configuration options relevant to each including the Label Items, Date Item, and Summary Items. IDataExtractors use fluent builders for all optional configurations. For example, creating a JsonDataExtractor would look like:

```
new JsonDataExtractor(labelAttributes)  
.withDateValueAttribute(dateAttribute)  
.withSummaryAttributes(summaryAttributes)  
.withDateFormat(dateFormat);  
```

##### Aggregator

You then create the Aggregator with options specific to it, including the KinesisClientLibConfiguration, the required TimeHorizons, and options around emitting metrics. For example, using our above JsonDataExtractor, we might configure the Aggregator with:

```
return new StreamAggregator(streamName, appName, namespace, config, dataExtractor)  
.withTimeHorizon(timeHorizons)  
.withAggregatorType(aggregatorType)  
.withCloudWatchMetrics(true);  
```

## Extending Aggregators

You may find that you want to extend Aggregators for a variety of reasons. The use cases that we know of today that will require extension include supporting data on a Stream which is compressed, encrypted, uses an Object serialisation format other than Jackson/JSON, or if you wanted to implement large objects. Aggregators was designed with extensibility in mind, and you can extend the framework at the following integration points:

### Data Format & Handling

The ability to support CSV, JSON, arbitrary String data and Object Serialisation is provided by IDataExtractor and IKinesisSerialiser interfaces, residing at com.amazonaws.services.kinesis.aggregators.io and io.serialiser.

#### IKinesisSerialiser

This interface is responsible for interoperating between the internal data format used by IDataExtractors, and byte arrays which are used on the Kinesis Stream. You would build an IKinesisSerialiser implementation if you wanted to support compressed stream data, or if your data was encrypted, for example. The implementation would conform to the following interface, which is identical to the Kinesis Connectors ITransformer class:

```
/**  
 * IKinesisSerialiser is used to transform data from a Record (byte array) to the data  
 * model class (T) for processing in the application and from the data model  
 * class to the output type (U) for the emitter.  
 * 
 * @param <T> the data type stored in the record  
 */
public interface IKinesisSerialiser<T, U> {  
    /**
     * Transform record into an object of its original class.  
     * 
     * @param record raw record from the Kinesis stream  
     * @return data as its original class  
     * @throws IOException could not convert the record to a T  
     */
    public T toClass(InputEvent event) throws IOException;  

    /**
     * Transform record from its original class to byte array.  
     * 
     * @param record data as its original class  
     * @return data byte array  
     */
    public U fromClass(T record) throws IOException;  
}  
```
#### IDataExtractor

IDataExtractors are responsible for taking the deserialised data, and extracting the relevant Label, Date and Summary items out of the raw dataset. They also typically do any Filtering that is exposed by the IDataExtractor. You would build a new IDataExtractor if the type of data returned by a custom IKinesisSerialiser implementation was not compatible with the existing IDataExtractors in the io package. This new IDataExtractor would conform to:

```
/**
 * Interface which allows for pluggable data extractors for different types of
 * stream data. Aggregators use IDataExtractor to interoperate between the
 * stream data format and the internal format required for Aggregation.
 * IDataExtractors likely use IKinesisSerialisers to read and write to and from
 * the stream
 */
public interface IDataExtractor {  
    /**  
     * Get the name of the label value to be extracted.  
     *   
     * @return  
     */  
    public String getAggregateLabelName();  

    /**  
     * Get the name of the date value to be extracted.  
     *   
     * @return  
     */  
    public String getDateValueName();  

    /**
     * Extract one or more aggregatable items from a Kinesis Record.  
     *  
     * @param event The Kinesis Record data from which we want to extract data.  
     * @return A list of ExtractedData elements which have been resolved from  
     *         the input data.  
     * @throws SerialisationException  
     */
    public List<AggregateData> getData(InputEvent event) throws SerialisationException;

    /**
     * Set the type of aggregator which contains this IDataExtractor. Used to
     * boost efficiency in that the Extractor will not extract summary items for
     * COUNT based Aggregator integration.
     * 
     * @param type
     */
    public void setAggregatorType(AggregatorType type);

    /**
     * Validate that the extractor is well formed.
     * 
     * @throws Exception
     */
    public void validate() throws Exception;

    /**
     * Get the summary configuration that is driving data extraction against the
     * data stream.
     * 
     * @return
     */
    public SummaryConfiguration getSummaryConfig();

    public IDataExtractor copy() throws Exception;
}
```

Also note that an IDataExtractor returns multiple Aggregatable objects from the Stream. If you had a requirement to support M:N Kinesis Events to Aggregatable Events, an IDataExtractor could do the job with local state.

Finally, please note that the IDataExtractor is STATEFUL for the life of an Aggregator running on a Shard, and contains the configuration of what data is to be extracted. Because a new IDataExtractor will be generated when a new Aggregator is initialised on a shard, you must ensure that it is Thread safe and implement the copy() interface correctly to ensure that multiple instances can operate within a single JVM.

### Data Store

The Aggregators framework backs its data onto Dynamo DB, and takes advantage of powerful Dynamo DB features such as Hash/Range keys, atomic increment and conditional updates. It also implements a defensive flush mechanism which means that at any provisioned IO Rate, the Aggregator will be able to flush its state to Dynamo DB without timing out.

If you would like to extend Aggregators with support for an alternate backing store, such as a relational database or Redis, you would supply an implementation of com.amazonaws.services.kinesis.aggregators.datastore.IDataStore. This implementation would have to ensure that it could meet the following service levels:

* Ability to flush all internal state to the datastore in 5 minutes or less (this is due to the Kinesis Worker Timeout)
* Ability to support a composite primary key for all label values and date value
* Ability to do an atomic, transactional increment operation
* Ability to conditionally update a discrete value in the table

The implementation of a new IDataStore would conform to:

```
/**
 * Interface which is used to allow the in memory cached aggregates to be saved
 * to a persistent store
 */
public interface IDataStore {
    /**
     * Write a set of Update Key/Value pairs back to the backing store
     * 
     * @param data The Input Dataset to be updated
     * @return A data structure which maps a set of
     *         AggregateAttributeModifications back to the values that were
     *         affected on the underlying datastore, by UpdateKey
     * @throws Exception
     */
    public Map<UpdateKey, Map<String, AggregateAttributeModification>> write(
            Map<UpdateKey, UpdateValue> data) throws Exception;

    /**
     * Method called on creation of the IDataStore
     * 
     * @throws Exception
     */
    public void initialise() throws Exception;

    /**
     * Method which will be periodically invoked to allow the IDataStore to
     * refresh tolerated limits for how often write() should be called
     * 
     * @return
     * @throws Exception
     */
    public long refreshForceCheckpointThresholds() throws Exception;

    /**
     * Method called to set the region for the IDataStore
     * 
     * @param region
     */
    public void setRegion(Region region);
}
```

### Metrics Service

By default Aggregators only has the ability to integrate with CloudWatch for the purposes of Metrics dashboarding and alerting. However, it is reasonable to consider wanting to push metrics to platforms such as Ganglia or New Relic. In these cases, you would provide an implementation of the com.amazonaws.services.kinesis.aggregators.metrics.IMetricsEmitter. This implementation would conform to:

```
/**
 * Interface for providing classes which can write to metrics services. It
 * receives the output of the IDataStore modifications, and applies the data to
 * the metrics service
 */
public interface IMetricsEmitter {
    /**
     * Emit a new set of metrics to the metrics service
     * 
     * @param metricData Input Data to be intrumented
     * @throws Exception
     */
    public void emit(Map<UpdateKey, Map<String, AggregateAttributeModification>> metricData)
            throws Exception;

    /**
     * Method called to indicate the Region of the metrics service
     * 
     * @param region
     */
    public void setRegion(Region region);
}
```
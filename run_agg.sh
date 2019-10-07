#!/bin/bash
java -cp /AmazonKinesisAggregators.jar-complete.jar -Dstream-name=test -Dapplication-name=kinesis_agg -Dconfig-file-path="/etc/kinesis-aggregators/json-aggregator.json" com.amazonaws.services.kinesis.aggregators.consumer.AggregatorConsumer

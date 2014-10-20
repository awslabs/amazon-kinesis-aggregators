#!/bin/bash

# Number of messages for this producer to create
num_messages=$1

# Format should be one of 'json','csv', or 'string'
format=$2

# Stream to write messages to
stream=$3

# AWS Region Name to use, such as 'us-east-1' or 'eu-west=1'. US East is Default
region=$4

java -cp ../../target/AmazonKinesisAggregators.jar-complete.jar producer.SensorReadingProducer $num_messages $format $stream $region
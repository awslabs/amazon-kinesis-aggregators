#!/bin/bash
#
# Amazon Kinesis Aggregators
#
# Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Amazon Software License (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#  http://aws.amazon.com/asl/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.
#


# Number of messages for this producer to create
num_messages=$1

# Format should be one of 'json','csv', or 'string'
format=$2

# Stream to write messages to
stream=$3

# AWS Region Name to use, such as 'us-east-1' or 'eu-west=1'. US East is Default
region=$4

java -cp ../../target/AmazonKinesisAggregators.jar-complete.jar producer.SensorReadingProducer $num_messages $format $stream $region
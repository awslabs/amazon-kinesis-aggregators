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
package com.amazonaws.services.kinesis.aggregators.idempotency;

import com.amazonaws.services.kinesis.aggregators.AggregateData;

/**
 * Default implementation of an Idempotency Check. Always returns True - that an
 * input element should be processed
 */
public class DefaultIdempotencyCheck implements IIdempotencyCheck {
    public DefaultIdempotencyCheck() {
    }

    public boolean doProcess(String partitionKey, String sequenceNumber, AggregateData dataElement,
            byte[] originalData) {
        return true;
    }
}

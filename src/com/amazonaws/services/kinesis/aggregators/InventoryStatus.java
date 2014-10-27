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
package com.amazonaws.services.kinesis.aggregators;

public class InventoryStatus {
    private String lastTime, lowSeq, highSeq;

    public InventoryStatus(String lastTime, String lowSeq, String highSeq) {
        super();
        this.lastTime = lastTime;
        this.lowSeq = lowSeq;
        this.highSeq = highSeq;
    }

    public String getLastTime() {
        return lastTime;
    }

    public String getLowSeq() {
        return lowSeq;
    }

    public String getHighSeq() {
        return highSeq;
    }
}

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
package com.amazonaws.services.kinesis.aggregators.datastore;

import java.util.Map;

import com.amazonaws.regions.Region;
import com.amazonaws.services.kinesis.aggregators.cache.UpdateKey;
import com.amazonaws.services.kinesis.aggregators.cache.UpdateValue;

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

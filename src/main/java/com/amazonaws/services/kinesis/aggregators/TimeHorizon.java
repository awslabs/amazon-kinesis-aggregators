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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public enum TimeHorizon {
    SECOND(0, 1000, "s"), MINUTE(1, 60000, "m"), MINUTES_GROUPED(1, 60000, "mb"),
    HOUR(2, 3600000L, "H"), DAY(3, 86400000L, "d"), WEEK(4, 604800000L, "W"), MONTH(5, 2592000000L, "M"), YEAR(
            6, 31536000000L, "Y"), FOREVER(999, -1, "*") {
        /**
         * Override the getValue method, as TimeHorizon.FOREVER is for all
         * values regardless of time period. We'll set the value to '*' as
         * Dynamo wont allow an empty value
         */
        @Override
        public String getValue(Date forDate) {
            return "*";
        }
    };

    private TimeHorizon(int placemark, long resolution, String abbrev) {
        this.placemark = placemark;
        this.resolution = resolution;
        this.abbrev = abbrev;
        this.chunks = 1;
    }

    private int placemark;

    private long chunks;
    private long resolution;

    private String abbrev;

    public String getAbbrev() {
        return this.abbrev;
    }

    public String getItemWithMultiValueFormat(Date dateValue) {
        return getAbbrev() + "-" + getValue(dateValue);
    }

    public String getValue(Date forDate) {
        long timestamp = forDate.getTime();
        long bucket = (timestamp / (this.resolution * this.chunks)) * (this.resolution * this.chunks); // int div to round
        return Long.toString(bucket);
    }

    /**
     * Returns the full hierarchy of TimeHorizon values from this Horizon to
     * FOREVER
     * 
     * @return
     */
    public List<TimeHorizon> getFullHierarchy() {
        return getHierarchyTo(TimeHorizon.FOREVER);
    }

    /**
     * Get a list of all TimeHorizons in decreasing granularity, to the
     * indicated Time Horizon. For example, if we requested
     * TimeHorizon.MINUTE.getHierarchyTo(TimeHorizon.MONTH), we would receive a
     * list of MINUTE, HOUR, DAY, MONTH
     * 
     * @param t
     * @return
     */
    public List<TimeHorizon> getHierarchyTo(TimeHorizon t) {
        List<TimeHorizon> hierarchy = new ArrayList<>();

        for (TimeHorizon h : TimeHorizon.values()) {
            // don't include Minutes Group in automated hierarchies as they are
            // a peer to Minutes
            if (h.placemark >= this.placemark && h.placemark <= t.placemark
                    && !h.equals(TimeHorizon.MINUTES_GROUPED)) {
                hierarchy.add(h);
            }
        }

        return hierarchy;
    }

    public long getGranularity() throws Exception {
        return this.chunks;
    }

    public void setGranularity(long chunks) throws Exception {
        this.chunks = chunks;
    }
}

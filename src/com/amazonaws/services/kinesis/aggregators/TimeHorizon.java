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
    SECOND(0, "MM-dd HH:mm:ss", "s"), MINUTE(1, "MM-dd HH:mm:00", "m"), MINUTES_GROUPED(1, null,
            "mb") {
        private Calendar calendar = Calendar.getInstance();

        private int scope;

        @Override
        public int getGranularity() {
            return this.scope;
        }

        @Override
        public void setGranularity(int bucketSize) {
            this.scope = bucketSize;
        }

        @Override
        public String getValue(Date forDate) {
            calendar.setTime(forDate);
            int minutes = calendar.get(Calendar.MINUTE);
            int bucket = new Double(Math.floor(minutes / scope) * scope).intValue();

            return String.format("%s:%02d:00",
                    new SimpleDateFormat("yyyy-MM-dd HH").format(forDate), bucket);
        }
    },
    HOUR(2, "MM-dd HH:00:00", "H"), DAY(3, "MM-dd 00:00:00", "d"), WEEK(4, "ww", "W"), MONTH(5, "MM-01 00:00:00", "M"), YEAR(
            6, "01-01 00:00:00", "Y"), FOREVER(999, "", "*") {
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

    private TimeHorizon(int placemark, String mask, String abbrev) {
        this.placemark = placemark;
        this.mask = mask;
        this.abbrev = abbrev;
    }

    private int placemark;

    private String mask;

    private String abbrev;

    private SimpleDateFormat getMask() {
        return new SimpleDateFormat("yyyy-" + this.mask);
    }

    public String getAbbrev() {
        return this.abbrev;
    }

    public String getItemWithMultiValueFormat(Date dateValue) {
        return getAbbrev() + "-" + getValue(dateValue);
    }

    public String getValue(Date forDate) {
        return getMask().format(forDate);
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

    public int getGranularity() throws Exception {
        throw new Exception("Not Implemented");
    }

    public void setGranularity(int scope) throws Exception {
        throw new Exception("Not Implemented");
    }
}

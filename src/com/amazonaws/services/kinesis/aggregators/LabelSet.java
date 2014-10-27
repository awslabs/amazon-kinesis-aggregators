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

import java.util.LinkedHashMap;
import java.util.List;

/**
 * Object which encapsulates all label values to be tracked and managed within
 * the Aggregator framework. Adheres to the properties of strict ordering by
 * insert sequence, equality on the basis of values as well as hash on the basis
 * of values, and a name property synthesized from the keyset, and a String
 * value synthesized from the value set
 */
public class LabelSet extends LinkedHashMap<String, String> {
    private final String setDelimiter = ".";

    private String alias = null;

    public LabelSet() {
        super();
    }

    public static LabelSet fromIntegerKeys(List<Integer> keys) {
        LabelSet labels = new LabelSet();
        for (Integer i : keys) {
            labels.put("" + i, null);
        }
        return labels;
    }

    public static LabelSet fromStringKeys(List<String> keys) {
        LabelSet labels = new LabelSet();
        for (String s : keys) {
            labels.put(s, null);
        }
        return labels;
    }

    @Override
    public String put(String key, String value) {
        // wrap general map put with internal pre-processing of names
        return super.put(StreamAggregatorUtils.methodToColumn(key), value);
    }

    public String valuesAsString() {
        StringBuffer sb = new StringBuffer();
        for (String s : this.values()) {
            sb.append(s + setDelimiter);
        }

        return sb.substring(0, sb.length() - 1);
    }

    public String getName() {
        if (this.alias == null) {
            StringBuffer sb = new StringBuffer();
            for (String s : this.keySet()) {
                sb.append(StreamAggregatorUtils.methodToColumn(s) + setDelimiter);
            }

            return sb.substring(0, sb.length() - 1);
        } else {
            return this.alias;
        }
    }

    public LabelSet withAlias(String alias) {
        this.alias = alias;

        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null)
            return false;

        if (!(o instanceof LabelSet))
            return false;

        LabelSet other = (LabelSet) o;
        boolean matched = false;

        // match on keys
        for (String s : this.keySet()) {
            matched = false;
            for (String k : other.keySet()) {
                if (k.equals(s)) {
                    matched = true;
                    break;
                }
            }
            if (!matched)
                return false;
        }

        // must match on values
        for (String t : this.values()) {
            matched = false;
            for (String v : other.values()) {
                if (t.equals(v)) {
                    matched = true;
                    break;
                }
            }
            if (!matched)
                return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int res = 17;
        for (String s : this.keySet()) {
            res = 31 * res + (s == null ? 0 : s.hashCode());
        }

        for (String t : this.values()) {
            res = 31 * res + (t == null ? 0 : t.hashCode());
        }
        return res;
    }
}

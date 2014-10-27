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
package com.amazonaws.services.kinesis.io;

import java.util.List;

import com.amazonaws.services.kinesis.io.serializer.CsvSerializer;
import com.amazonaws.services.kinesis.io.serializer.RegexSerializer;

/**
 * IDataExtractor implementation which allows for extraction of data from
 * Streams formatted as Character Separated Values. Also optionally allows for
 * regular expression based filtering of the stream prior to aggregation.
 */
public class RegexDataExtractor extends StringDataExtractor<RegexDataExtractor> implements
        IDataExtractor {
    private String regex;

    private RegexSerializer serialiser;

    /**
     * Create a new data extractor using the indicated index for the label value
     * to be aggregated on, and the regular expression used for data extraction
     * 
     * @param labelIndex Index (base 0) of where in the CSV stream the label
     *        value occurs
     * @param delimiter The character delimiter separating items in the stream
     *        data.
     */
    public RegexDataExtractor(String regex, List<Integer> labelIndicies) {
        this(regex, labelIndicies, null, -1, null, null);
    }

    public RegexDataExtractor(String regex, List<Integer> labelIndicies, int dateValueIndex) {
        this(regex, labelIndicies, null, dateValueIndex, null, null);
    }

    public RegexDataExtractor(String regex, List<Integer> labelIndicies,
            String labelAttributeAlias, int dateValueIndex, String dateAttributeAlias,
            RegexSerializer serialiser) {
        this.regex = regex;
        super.labelIndicies = labelIndicies;
        super.labelAttributeAlias = labelAttributeAlias;
        super.dateAttributeAlias = dateAttributeAlias;

        if (dateValueIndex != -1)
            super.dateValueIndex = dateValueIndex;
        if (serialiser != null) {
            super.serialiser = serialiser;
        } else {
            super.serialiser = new RegexSerializer(regex);
        }
    }

    /**
     * Add a non default item terminator. The default is "\n"
     * 
     * @param lineTerminator The characters used for delimiting lines of text
     * @return
     */
    public RegexDataExtractor withItemTerminator(String lineTerminator) {
        if (lineTerminator != null) {
            this.serialiser.withItemTerminator(lineTerminator);
            super.serialiser = this.serialiser;
        }
        return this;
    }

    /**
     * Add a custom configured serialiser
     * 
     * @param serialiser
     * @return
     */
    public RegexDataExtractor withSerialiser(CsvSerializer serialiser) {
        super.serialiser = serialiser;
        return this;
    }

    /**
     * Builder method for adding a index to the extraction configuration which
     * indicates where the date item to be used for aggregation can be found.
     * 
     * @param dateValueIndex The index value (base 0) in the CSV stream which
     *        contains the date value.
     * @return
     */
    public RegexDataExtractor withDateValueIndex(Integer dateValueIndex) {
        if (dateValueIndex != null) {
            this.dateValueIndex = dateValueIndex;
        }
        return this;
    }

    @Override
    public IDataExtractor copy() throws Exception {
        RegexDataExtractor dataExtractor = new RegexDataExtractor(this.regex, this.labelIndicies,
                super.labelAttributeAlias, this.dateValueIndex, super.dateAttributeAlias,
                this.serialiser).withSummaryIndicies(this.getOriginalSummaryExpressions());

        return dataExtractor;
    }
}

/**
 * Amazon Kinesis Aggregators Copyright 2014, Amazon.com, Inc. or its
 * affiliates. All Rights Reserved. Licensed under the Amazon Software License
 * (the "License"). You may not use this file except in compliance with the
 * License. A copy of the License is located at http://aws.amazon.com/asl/ or in
 * the "license" file accompanying this file. This file is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.amazonaws.services.kinesis.io;

import java.util.List;

import com.amazonaws.services.kinesis.io.serializer.CsvSerializer;

/**
 * IDataExtractor implementation which allows for extraction of data from
 * Streams formatted as Character Separated Values. Also optionally allows for
 * regular expression based filtering of the stream prior to aggregation.
 */
public class CsvDataExtractor extends StringDataExtractor<CsvDataExtractor> implements
        IDataExtractor {
    private static String delimiter = ",";

    private static String itemTerminator = "\n";

    private CsvSerializer serialiser;

    /**
     * Create a new data extractor using the indicated index for the label value
     * to be aggregated on, and the delimiter for tokenising the data value.
     * 
     * @param labelIndex Index (base 0) of where in the CSV stream the label
     *        value occurs
     * @param delimiter The character delimiter separating items in the stream
     *        data.
     */
    public CsvDataExtractor(List<Integer> labelIndicies) {
        super.labelIndicies = labelIndicies;
        this.serialiser = new CsvSerializer().withFieldDelimiter(delimiter).withItemTerminator(
                itemTerminator);
        super.serialiser = serialiser;
    }

    public CsvDataExtractor(List<Integer> labelIndicies, String labelAttributeAlias,
            int dateValueIndex, String dateAttributeAlias, String fieldDelimiter,
            CsvSerializer serialiser) {
        super.labelIndicies = labelIndicies;
        super.labelAttributeAlias = labelAttributeAlias;
        super.dateValueIndex = dateValueIndex;
        super.dateAttributeAlias = dateAttributeAlias;
        this.serialiser = serialiser;
        super.serialiser = serialiser;
    }

    /**
     * Add a regular expression filter to this data extractor. When configured,
     * only string values which match the regular expression will be
     * deserialised and have data extracted from it.
     * 
     * @param filterRegex Regular expression which must match in order for data
     *        to be subject to data extraction.
     * @return
     */
    public CsvDataExtractor withRegexFilter(String filterRegex) {
        if (filterRegex != null) {
            this.serialiser.withFilterRegex(filterRegex);
            super.serialiser = this.serialiser;
        }
        return this;
    }

    /**
     * Add a non default field delimiter. The default is ","
     * 
     * @param delimiter The characters used for delimiting items within a line
     * @return
     */
    public CsvDataExtractor withDelimiter(String delimiter) {
        if (delimiter != null) {
            this.serialiser.withFieldDelimiter(delimiter);
            super.serialiser = this.serialiser;
        }
        return this;
    }

    /**
     * Add a non default item terminator. The default is "\n"
     * 
     * @param lineTerminator The characters used for delimiting lines of text
     * @return
     */
    public CsvDataExtractor withItemTerminator(String lineTerminator) {
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
    public CsvDataExtractor withSerialiser(CsvSerializer serialiser) {
        this.serialiser = serialiser;
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
    public CsvDataExtractor withDateValueIndex(Integer dateValueIndex) {
        if (dateValueIndex != null) {
            this.dateValueIndex = dateValueIndex;
        }
        return this;
    }

    @Override
    public IDataExtractor copy() throws Exception {
        return new CsvDataExtractor(this.labelIndicies, super.labelAttributeAlias,
                this.dateValueIndex, super.dateAttributeAlias, this.delimiter, this.serialiser).withSummaryIndicies(this.getOriginalSummaryExpressions());
    }
}

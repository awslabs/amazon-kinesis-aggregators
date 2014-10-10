package com.amazonaws.services.kinesis.io.serialiser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import com.amazonaws.services.kinesis.aggregators.InputEvent;

public class CsvSerialiser extends StringSerialiser<CsvSerialiser> implements
        IKinesisSerialiser<List<List<String>>, byte[]> {
    private String delimiter = ",";

    private String itemTerminator = "\n";

    private String filterRegex;

    private String charset = "UTF-8";

    private Pattern p;

    /**
     * Convert a Kinesis record into one or more String lists by tokenising the
     * parsed item by the delimiter
     */
    public List<List<String>> toClass(InputEvent event) throws IOException {
        List<List<String>> outputData = new ArrayList<>();
        List<String> item = new ArrayList<>();

        try {
            String[] lines;

            lines = super.getItems(event);

            // apply filters and tokenise by delimiter
            for (String line : lines) {
                if ((filterRegex != null && p.matcher(line).matches()) || filterRegex == null) {
                    item = Arrays.asList(line.split(delimiter));
                    outputData.add(item);
                }
            }

            return outputData;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * Generate a byte stream in the supplied character set using the String
     * list of CSV items
     */
    public byte[] fromClass(List<List<String>> csv) throws IOException {
        StringBuffer ret = new StringBuffer();
        StringBuffer sb = new StringBuffer();
        for (List<String> item : csv) {
            for (String s : item) {
                sb.append(s + this.delimiter);
            }
            ret.append(sb.substring(0, sb.length() - 1) + this.itemTerminator);
            sb = new StringBuffer();
        }

        return SerialisationUtils.safeReturnData(ret.substring(0, ret.length() - 1).getBytes(
                this.charset));
    }

    /**
     * Builder method to apply a non-default field delimiter (default ',')
     * 
     * @param delimiter
     * @return
     */
    public CsvSerialiser withFieldDelimiter(String delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    /**
     * Builder method to apply a filtering regular expression to text based
     * serialisation operations
     * 
     * @param regex
     * @return
     */
    public CsvSerialiser withFilterRegex(String regex) {
        this.filterRegex = regex;
        p = Pattern.compile(this.filterRegex);

        return this;
    }
}

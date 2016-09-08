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
package com.amazonaws.services.kinesis.io.serializer;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.amazonaws.services.kinesis.aggregators.InputEvent;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

@SuppressWarnings("rawtypes")
/**
 * Class which handles serialising Object payloads using Jackson marshalling, or converts to string format if configured to support text based payloads
 */
public class JsonSerializer implements IKinesisSerializer<Object, byte[]> {
    ObjectMapper mapper = new ObjectMapper();

    String itemTerminator = null;

    Class clazz;

    private String filterRegex;

    private Pattern p;

    private String charset = "UTF-8";

    /**
     * Construct a basic json data serialiser
     */
    public JsonSerializer() {
    }

    /**
     * Construct a serialiser that is based on a densely packed recordset list
     * of items
     * 
     * @param itemTerminator
     */
    public JsonSerializer(String itemTerminator) {
        this.itemTerminator = itemTerminator;
    }

    /**
     * Construct a Serialiser which is class based
     * 
     * @param clazz
     */
    public JsonSerializer(Class clazz) {
        this.clazz = clazz;
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @SuppressWarnings("unchecked")
    /**
     * Method to generate either a class instance from a Kinesis Record, or a String which will be converted to JsonMap if we are serialising text based payloads
     */
    public Object toClass(final InputEvent event) throws IOException {
        // Return a class object from the json, or if we have no class then
        // return a String list
        List<String> jsonStringList = new ArrayList<>();

        if (this.clazz == null) {
            if (this.itemTerminator != null) {
                // break up the json items as separate lines
                String[] items = new String(event.getData(), this.charset).split(this.itemTerminator);

                for (String item : items) {
                    if (filterRegex == null || (filterRegex != null && p.matcher(item).matches())) {
                        jsonStringList.add(item);
                    }
                }

                return jsonStringList;
            } else {
                // single json object per record
                String item = new String(event.getData(), this.charset);

                if (filterRegex == null || (filterRegex != null && p.matcher(item).matches())) {
                    jsonStringList.add(item);
                }

                return jsonStringList;
            }
        } else {
            // use jackson to serialise a class instance
            return mapper.readValue(event.getData(), clazz);
        }
    }

    /**
     * Convert a given object into the required binary representation, based
     * upon the serialiser config as either an object serialiser or a string
     * serialiser
     */
    public byte[] fromClass(final Object o) throws IOException {
        if (this.clazz == null) {
            return SerializationUtils.safeReturnData(((String) o).getBytes(this.charset));
        } else {
            return SerializationUtils.safeReturnData(mapper.writeValueAsBytes(o));
        }
    }

    /**
     * Builder method to apply a filtering regular expression to text based
     * serialisation operations
     * 
     * @param regex
     * @return
     */
    public JsonSerializer withFilterRegex(String regex) {
        this.filterRegex = regex;
        p = Pattern.compile(this.filterRegex);

        return this;
    }

    /**
     * Builder method to apply a non-default character set to text based
     * serialisation operations (default UTF-8)
     * 
     * @param charset
     * @return
     */
    public JsonSerializer withCharset(String charset) {
        // test that this is a valid character set
        Charset test = Charset.forName(charset);

        // use it
        this.charset = charset;

        return this;
    }

    /**
     * Build method to apply a non-default item terminator (default \n)
     * 
     * @param itemTerminator
     * @return
     */
    public JsonSerializer withItemTerminator(String itemTerminator) {
        this.itemTerminator = itemTerminator;
        return this;
    }
}

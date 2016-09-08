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
package com.amazonaws.services.kinesis.aggregators.app;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.amazonaws.services.kinesis.aggregators.AggregatorsConstants;
import com.amazonaws.services.kinesis.aggregators.configuration.ConfigFileUtils;

public class FetchConfigurationServlet extends AbstractQueryServlet {
    private void respondWith(HttpServletResponse response, Map<String, String> configItems)
            throws IOException {
        response.setStatus(200);
        // cors grant
        response.setHeader("Access-Control-Allow-Origin", "*");
        PrintWriter w = response.getWriter();

        int i = 0;

        // write out the response values as json
        w.println("{");

        int resultCount = 0;

        for (String s : configItems.keySet()) {
            resultCount++;

            String value = configItems.get(s);

            w.print(String.format("\"%s\":%s", s,
                    value == null ? "null" : String.format("\"%s\"", value)));

            if (resultCount != configItems.size()) {
                w.println(",");
            }
        }
        w.print("}");
    }

    @Override
    protected void doAction(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        try {
            Map<String, String> config = new HashMap<>();

            // required items
            config.put(AggregatorsConstants.REGION_PARAM,
                    System.getProperty(AggregatorsConstants.REGION_PARAM));
            config.put(AggregatorsConstants.STREAM_NAME_PARAM,
                    System.getProperty(AggregatorsConstants.STREAM_NAME_PARAM));
            config.put(AggregatorsConstants.APP_NAME_PARAM,
                    System.getProperty(AggregatorsConstants.APP_NAME_PARAM));
            config.put(AggregatorsConstants.CONFIG_URL_PARAM,
                    System.getProperty(AggregatorsConstants.CONFIG_URL_PARAM));
            config.put(
                    "fetch-config-url",
                    ConfigFileUtils.makeConfigFileURL(System.getProperty(AggregatorsConstants.CONFIG_URL_PARAM)));

            // optional items
            config.put(AggregatorsConstants.ENVIRONMENT_PARAM,
                    System.getProperty(AggregatorsConstants.ENVIRONMENT_PARAM));
            config.put(AggregatorsConstants.MAX_RECORDS_PARAM,
                    System.getProperty(AggregatorsConstants.MAX_RECORDS_PARAM));
            config.put(AggregatorsConstants.FAILURES_TOLERATED_PARAM,
                    System.getProperty(AggregatorsConstants.FAILURES_TOLERATED_PARAM));

            respondWith(response, config);
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }
}

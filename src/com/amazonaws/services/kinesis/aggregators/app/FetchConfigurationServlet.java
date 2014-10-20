package com.amazonaws.services.kinesis.aggregators.app;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.amazonaws.services.kinesis.aggregators.AggregatorsConstants;

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

        // optional items
        config.put(AggregatorsConstants.ENVIRONMENT_PARAM,
                System.getProperty(AggregatorsConstants.ENVIRONMENT_PARAM));
        config.put(AggregatorsConstants.MAX_RECORDS_PARAM,
                System.getProperty(AggregatorsConstants.MAX_RECORDS_PARAM));
        config.put(AggregatorsConstants.FAILURES_TOLERATED_PARAM,
                System.getProperty(AggregatorsConstants.FAILURES_TOLERATED_PARAM));

        respondWith(response, config);
    }
}

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
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.kinesis.aggregators.AggregatorGroup;
import com.amazonaws.services.kinesis.aggregators.AggregatorsConstants;
import com.amazonaws.services.kinesis.aggregators.StreamAggregator;
import com.amazonaws.services.kinesis.aggregators.TimeHorizon;

public class DateQueryServlet extends AbstractQueryServlet {
    public static final String NAMESPACE_PARAM = "namespace";

    public static final String DATE_VALUE_PARAM = "date-value";

    public static final String OPERATOR_PARAM = "operator";

    public static final String GRANULARITY_PARAM = "granularity";

    public static final int QUERY_THREADS = 10;

    private void respondWith(HttpServletResponse response,
            List<Map<String, AttributeValue>> queryResult) throws IOException {
        response.setStatus(200);
        // cors grant
        response.setHeader("Access-Control-Allow-Origin", "*");
        PrintWriter w = response.getWriter();
        w.println("[");

        int i = 0;

        // write out the response values as json
        if (queryResult != null) {
            for (Map<String, AttributeValue> map : queryResult) {
                i++;
                int j = 0;
                w.print("{");
                for (String s : map.keySet()) {
                    j++;

                    String toPrint = map.get(s).getS();

                    if (toPrint == null) {
                        toPrint = map.get(s).getN();
                    }

                    w.print(String.format("\"%s\":\"%s\"", s, toPrint));

                    if (j != map.keySet().size()) {
                        w.println(",");
                    }
                }
                w.print("}");

                if (i != queryResult.size()) {
                    w.println(",");
                }
            }
        }

        w.print("]");
    }

    public void doAction(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String namespace = request.getParameter(NAMESPACE_PARAM);
        String dateValue = request.getParameter(DATE_VALUE_PARAM);
        String operator = request.getParameter(OPERATOR_PARAM);
        String granularity = request.getParameter(GRANULARITY_PARAM);

        // create the date item
        Date d = null;
        try {
            d = StreamAggregator.dateFormatter.parse(dateValue);
        } catch (Exception e) {
            doError(response, String.format("Date Parameter must be in format %s",
                    StreamAggregator.dateFormatter.getDateFormatSymbols().toString()));
            return;
        }

        // create the ComparisonOperator for Dynamo from the argument
        ComparisonOperator c = null;
        try {
            c = ComparisonOperator.fromValue(operator);
        } catch (Exception e) {
            doError(response, String.format("%s is an invalid Comparison Operator", operator));
            return;
        }

        // create the Time Horizon value from the argument
        TimeHorizon h = null;
        try {
            h = TimeHorizon.valueOf(granularity);
        } catch (Exception e) {
            doError(response, String.format("%s is an invalid Granularity", granularity));
            return;
        }

        String streamName = (String) request.getServletContext().getAttribute(
                AggregatorsConstants.STREAM_NAME_PARAM);
        AggregatorGroup aggGroup = (AggregatorGroup) request.getServletContext().getAttribute(
                AggregatorsBeanstalkApp.AGGREGATOR_GROUP_PARAM);

        if (aggGroup == null) {
            doError(response, "Aggregator Application Not Initialised");
            return;
        }

        // initialise the aggregator group onto shard 'none' for this operation
        // - it may already be initialised
        try {
            aggGroup.initialize("none");
        } catch (Exception e) {
            throw new ServletException(e);
        }

        // put the initialised aggregator group back into the context
        request.getServletContext().setAttribute(AggregatorsBeanstalkApp.AGGREGATOR_GROUP_PARAM,
                aggGroup);

        // acquire the correct aggregator by namespace
        for (StreamAggregator agg : aggGroup.getAggregators()) {
            if (agg.getNamespace().equals(namespace)) {
                // run the query
                try {
                    respondWith(response, agg.queryByDate(d, h, c, QUERY_THREADS));
                    return;
                } catch (Exception e) {
                    throw new ServletException(e);
                }
            }
        }

        // shouldn't get here, so bail with a meaning error on namespace
        doError(response,
                String.format("Unable to acquire Aggregator with Namespace %s", namespace));
    }
}

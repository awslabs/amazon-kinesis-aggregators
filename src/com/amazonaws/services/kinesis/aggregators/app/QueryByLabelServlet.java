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
import java.text.ParseException;
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

public class QueryByLabelServlet extends AbstractQueryServlet {
    public static final String NAMESPACE_PARAM = "namespace";

    public static final String LABEL_VALUE_PARAM = "label-value";

    public static final String DATE_VALUE_PARAM = "date-value";

    public static final String OPERATOR_PARAM = "operator";

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
            int resultCount = 0;

            for (Map<String, AttributeValue> map : queryResult) {
                resultCount++;
                int mapCount = 0;
                if (map != null) {
                    w.println("{");

                    for (String s : map.keySet()) {
                        mapCount++;

                        if (map.get(s).getN() == null) {
                            w.print(String.format("\"%s\":\"%s\"", s, map.get(s).getS()));
                        } else {
                            w.print(String.format("\"%s\":%s", s, map.get(s).getN()));
                        }

                        if (mapCount != map.size()) {
                            w.println(",");
                        }
                    }
                    w.print("}");

                    if (resultCount != queryResult.size()) {
                        w.println(",");
                    }
                }
            }
        }

        w.print("]");
    }

    @Override
    protected void doAction(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String namespace = request.getParameter(NAMESPACE_PARAM);
        String labelValue = request.getParameter(LABEL_VALUE_PARAM);
        String dateValue = request.getParameter(DATE_VALUE_PARAM);
        String operator = request.getParameter(OPERATOR_PARAM);

        // have to provide namespace and label
        if (namespace == null) {
            doError(response, String.format("Argument '%s' must not be null", NAMESPACE_PARAM));
            return;
        }
        if (labelValue == null || labelValue.equals("")) {
            doError(response, String.format("Argument '%s' must not be null", LABEL_VALUE_PARAM));
            return;
        }

        // if date value is provided, the so too must operator and granularity
        ComparisonOperator setOperator = null;
        if (dateValue != null && operator == null) {
            setOperator = ComparisonOperator.EQ;
        }

        if (operator != null) {
            try {
                setOperator = ComparisonOperator.fromValue(operator);
            } catch (Exception e) {
                doError(response, String.format("%s is an invalid Comparison Operator", operator));
                return;
            }
        }

        String streamName = (String) request.getServletContext().getAttribute(
                AggregatorsConstants.STREAM_NAME_PARAM);
        AggregatorGroup aggGroup = (AggregatorGroup) request.getServletContext().getAttribute(
                AggregatorsBeanstalkApp.AGGREGATOR_GROUP_PARAM);
        if (aggGroup == null) {
            doError(response, "Aggregator Application Not Initialised");
            return;
        } else {
            // initialise the aggregator group onto shard 'none' for this
            // operation
            // - it may already be initialised
            try {
                aggGroup.initialize("none");
            } catch (Exception e) {
                throw new ServletException(e);
            }
        }

        Date dateValueAsDate = null;
        if (dateValue != null) {
            try {
                dateValueAsDate = StreamAggregator.dateFormatter.parse(dateValue);
            } catch (ParseException e1) {
                throw new ServletException(e1);
            }
        }

        // put the initialised aggregator group back into the context
        request.getServletContext().setAttribute(AggregatorsBeanstalkApp.AGGREGATOR_GROUP_PARAM,
                aggGroup);

        // acquire the correct aggregator by namespace
        for (StreamAggregator agg : aggGroup.getAggregators()) {
            if (agg.getNamespace().equals(namespace)) {
                // run the query
                try {
                    respondWith(response, agg.queryValue(labelValue, dateValueAsDate, setOperator));
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

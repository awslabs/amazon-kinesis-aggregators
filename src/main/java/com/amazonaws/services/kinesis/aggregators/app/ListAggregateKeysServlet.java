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
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.amazonaws.services.kinesis.aggregators.AggregatorGroup;
import com.amazonaws.services.kinesis.aggregators.AggregatorsConstants;
import com.amazonaws.services.kinesis.aggregators.StreamAggregator;
import com.amazonaws.services.kinesis.aggregators.TableKeyStructure;
import com.amazonaws.services.kinesis.aggregators.datastore.DynamoQueryEngine.QueryKeyScope;

public class ListAggregateKeysServlet extends AbstractQueryServlet {
    public static final String NAMESPACE_PARAM = "namespace";

    public static final String SCOPE_PARAM = "scope";

    public static final int QUERY_THREADS = 3;

    private void respondWith(HttpServletResponse response, List<TableKeyStructure> queryResult)
            throws IOException {
        response.setStatus(200);
        // cors grant
        response.setHeader("Access-Control-Allow-Origin", "*");
        PrintWriter w = response.getWriter();
        w.println("{");

        int i = 0;

        // write out the response values as json
        if (queryResult != null) {
            int result = 0;
            for (TableKeyStructure t : queryResult) {
                if (result == 0) {
                    w.println(String.format("\"labelName\":\"%s\",", t.getLabelAttributeName()));
                    w.println(String.format("\"dateName\":\"%s\",", t.getDateAttributeName()));
                    w.println("\"values\":[");
                }

                // write the value as a struct
                w.print("{");
                w.print(String.format("\"value\":\"%s\"", t.getLabelAttributeValue()));

                int dateItem = 0;
                if (t.getDateValues() != null) {
                    if (dateItem == 0) {
                        w.print(",\n\"dates\":[");
                    }

                    for (String s : t.getDateValues()) {
                        // write the date value
                        w.print(String.format("\"%s\"", s));

                        if (dateItem != t.getDateValues().size() - 1) {
                            w.println(",");
                        } else {
                            w.print("]");
                        }
                        dateItem++;
                    }
                }

                w.print("}");

                if (result != queryResult.size() - 1) {
                    w.println(",");
                }

                result++;
            }
        }

        w.print("]}");
    }

    @Override
    protected void doAction(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String namespace = request.getParameter(NAMESPACE_PARAM);
        String scope = request.getParameter(SCOPE_PARAM);

        // resolve the scope
        QueryKeyScope queryScope = null;
        try {
            queryScope = QueryKeyScope.valueOf(scope);
        } catch (Exception e) {
            doError(response, String.format("Invalid Query Scope %s", scope));
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
                    respondWith(response, agg.parallelQueryKeys(queryScope, QUERY_THREADS));
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

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
import java.net.URL;
import java.util.Date;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.kinesis.aggregators.AggregatorsConstants;
import com.amazonaws.services.kinesis.aggregators.configuration.ConfigFileUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;

public class ShowConfigFileServlet extends AbstractQueryServlet {
    private static final Log LOG = LogFactory.getLog(ShowConfigFileServlet.class);

    @Override
    protected void doAction(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        try {
            String configUrl = System.getProperty(AggregatorsConstants.CONFIG_URL_PARAM);
            String url = null;
            if (configUrl == null) {
                response.setStatus(404);
            } else {
                url = ConfigFileUtils.makeConfigFileURL(configUrl);
                LOG.info(String.format("Sending Redirect for Config File to S3 Temporary URL %s",
                        url));

                response.setHeader("Access-Control-Allow-Origin", "*");
                response.sendRedirect(url);
            }
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }
}

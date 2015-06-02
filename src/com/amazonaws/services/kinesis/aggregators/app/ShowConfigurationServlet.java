package com.amazonaws.services.kinesis.aggregators.app;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.amazonaws.services.kinesis.aggregators.AggregatorsConstants;
import com.amazonaws.services.kinesis.aggregators.StreamAggregator;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ShowConfigurationServlet extends AbstractQueryServlet {

	@Override
	protected void doAction(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		response.setStatus(200);
		// cors grant
		response.setHeader("Access-Control-Allow-Origin", "*");

		ObjectMapper mapper = new ObjectMapper();
		Map<String, String> configMap = new HashMap<>();

		configMap.put("version", StreamAggregator.version);
		configMap.put(AggregatorsConstants.STREAM_NAME_PARAM,
				System.getProperty(AggregatorsConstants.STREAM_NAME_PARAM));
		configMap.put(AggregatorsConstants.APP_NAME_PARAM,
				System.getProperty(AggregatorsConstants.APP_NAME_PARAM));
		configMap.put(AggregatorsConstants.REGION_PARAM,
				System.getProperty(AggregatorsConstants.REGION_PARAM));
		configMap.put(AggregatorsConstants.STREAM_POSITION_PARAM,
				System.getProperty(AggregatorsConstants.STREAM_POSITION_PARAM));
		configMap.put(AggregatorsConstants.MAX_RECORDS_PARAM,
				System.getProperty(AggregatorsConstants.MAX_RECORDS_PARAM));
		configMap.put(AggregatorsConstants.ENVIRONMENT_PARAM,
				System.getProperty(AggregatorsConstants.ENVIRONMENT_PARAM));
		configMap.put(AggregatorsConstants.FAILURES_TOLERATED_PARAM, System
				.getProperty(AggregatorsConstants.FAILURES_TOLERATED_PARAM));
		configMap.put(AggregatorsConstants.CONFIG_URL_PARAM, System
				.getProperty(AggregatorsConstants.CONFIG_URL_PARAM));

		PrintWriter w = response.getWriter();
		w.println(mapper.writeValueAsString(configMap));
	}

}

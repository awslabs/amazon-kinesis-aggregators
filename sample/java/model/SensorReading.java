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
package model;

import com.fasterxml.jackson.databind.ObjectMapper;

public class SensorReading {
	private static ObjectMapper mapper = new ObjectMapper();

	public enum OutputFormat {
		json, csv, string;
	}

	private OutputFormat outputAs = OutputFormat.json;

	private String id;
	private long captureTs;
	private String segment;
	private double lat;
	private double lng;
	private double pressure;
	private double temperature;
	private double flowRate;
	private double corrosionIndex;
	private double segmentIncline;

	private SensorReading() {
	}

	public SensorReading(String id, String segment, long captureTs, double lat,
			double lng, double pressure, double temperature, double flowRate,
			double corrosionIndex, double segmentIncline) {
		this.id = id;
		this.segment = segment;
		this.captureTs = captureTs;
		this.lat = lat;
		this.lng = lng;
		this.pressure = pressure;
		this.temperature = temperature;
		this.flowRate = flowRate;
		this.corrosionIndex = corrosionIndex;
		this.segmentIncline = segmentIncline;
	}

	public String getId() {
		return this.id;
	}

	public String getSegment() {
		return this.segment;
	}

	public long getCaptureTs() {
		return this.captureTs;
	}

	public double getLat() {
		return this.lat;
	}

	public double getLng() {
		return this.lng;
	}

	public double getPressure() {
		return this.pressure;
	}

	public double getTemp() {
		return this.temperature;
	}

	public double getFlowRate() {
		return this.flowRate;
	}

	public double getCorrosionIndex() {
		return this.corrosionIndex;
	}

	public double getSegmentIncline() {
		return this.segmentIncline;
	}

	public SensorReading withOutputFormat(OutputFormat format) {
		this.outputAs = format;
		return this;
	}

	public String asJson() throws Exception {
		return mapper.writeValueAsString(this);
	}

	public String asString() throws Exception {
		return String.format("%s (%s) ts-%s %sx%s %s at %s T:%s c:%10f deg%10f",
				this.id, this.segment, this.captureTs, this.lat, this.lng,
				this.pressure, this.flowRate, this.temperature,
				this.corrosionIndex, this.segmentIncline);
	}

	public String asCSV() throws Exception {
		return String.format("%s|%s|%s|%s|%s|%s|%s|%s|%10f|%10f", this.id,
				this.segment, this.captureTs, this.lat, this.lng,
				this.pressure, this.temperature, this.flowRate,
				this.corrosionIndex, this.segmentIncline);
	}

	@Override
	public String toString() {
		try {
			switch (this.outputAs) {
			case string:
				return this.asString();
			case csv:
				return this.asCSV();
			default:
				return this.asJson();
			}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
}

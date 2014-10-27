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

public class SensorState {
	private String segment;
	private double lat;
	private double lng;
	private double pressure;
	private double flowRate;
	private double temp;
	private double corrosion;
	private double incline;

	public SensorState(String segment, double lat, double lng, double pressure,
			double flowRate, double temp, double corrosion, double incline) {
		this.segment = segment;
		this.lat = lat;
		this.lng = lng;
		this.pressure = pressure;
		this.flowRate = flowRate;
		this.temp = temp;
		this.corrosion = corrosion;
		this.incline = incline;
	}

	public String getSegment() {
		return segment;
	}

	public double getLat() {
		return lat;
	}

	public double getLng() {
		return lng;
	}

	public double getPressure() {
		return pressure;
	}

	public double getFlowRate() {
		return flowRate;
	}

	public double getTemp() {
		return temp;
	}

	public double getCorrosion() {
		return corrosion;
	}

	public double getIncline() {
		return incline;
	}
}

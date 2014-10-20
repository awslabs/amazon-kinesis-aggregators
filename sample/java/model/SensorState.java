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

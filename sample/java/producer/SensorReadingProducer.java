package producer;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import model.SensorReading;
import model.SensorReading.OutputFormat;
import model.SensorState;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.PutRecordRequest;

public class SensorReadingProducer {
	private final Random rand = new SecureRandom();

	private final int ID_SPACE_SIZE = 1000;
	private final int NUM_SEGMENTS = 40;
	private final double PRESSURE_BASE = 108D;
	private final double PRESSURE_VOLATILITY = 3D;
	private final double FLOW_BASE = 1D;
	private final double FLOW_VOLATILITY = 1D;
	private final double TEMP_BASE = 16D;
	private final double TEMP_VOLATILITY = .2D;
	private final double INCLINE_BASE = 0D;
	private final double CORROSION_BASE = .0000234D;
	private final int BATCH_SIZE = 20;
	private final int BACKOFF = 5;
	private int sensorsGenerated = 0;

	private Map<String, SensorState> sensorCache = new HashMap<>();

	final double londonLat = 51.50722D;
	final double londonLng = -0.12750D;
	final double aberdeenLat = 57.1436900D;
	final double aberdeenLng = -2.0981400D;
	final double lineATan = Math.atan((aberdeenLng - londonLng)
			/ (aberdeenLat - londonLat));
	final double lineLength = Math.sqrt((Math.pow(aberdeenLng - londonLng, 2))
			+ (Math.pow(aberdeenLat - londonLat, 2)));
	final double lineIncrement = lineLength / NUM_SEGMENTS;

	public SensorReadingProducer() {
	}

	public double[] getLinePoint() {
		// random distance
		double dist = Math.random() * lineLength;

		// calculate which segment the point is in
		double seg = Math.floor(dist / lineIncrement) + 1;

		// derive lat/lng
		double lat = (dist / Math.cos(lineATan)) + londonLat;
		double lng = (dist * Math.sin(lineATan)) + londonLng;

		return new double[] { lat, lng, seg };
	}

	public SensorReading nextSensorReading(final OutputFormat format) {
		return nextSensorReading(format, rand.nextInt(ID_SPACE_SIZE));
	}

	public SensorReading nextSensorReading(final OutputFormat format,
			int position) {
		String id = Integer.toHexString(position);

		SensorState sensorState = sensorCache.get(id);
		if (sensorState == null) {
			sensorsGenerated++;

			System.out.println(String.format("Generating Sensor %s",
					sensorsGenerated));

			double[] location = getLinePoint();

			String segment = Integer.toHexString(new Double(location[2])
					.intValue());

			double pressure = PRESSURE_BASE
					+ (PRESSURE_VOLATILITY * rand.nextDouble());
			double flow = FLOW_BASE + (FLOW_VOLATILITY * rand.nextDouble());
			double temp = TEMP_BASE + (TEMP_VOLATILITY * rand.nextDouble());
			double corrosion = CORROSION_BASE + (rand.nextDouble() / 1000);
			double incline = INCLINE_BASE + (rand.nextDouble() / 1_000_000);

			sensorState = new SensorState(segment, location[0], location[1],
					pressure, flow, temp, corrosion, incline);

			sensorCache.put(id, sensorState);
		}

		double pressure = sensorState.getPressure()
				+ (PRESSURE_VOLATILITY * rand.nextDouble());
		double temp = sensorState.getTemp()
				+ (TEMP_VOLATILITY * rand.nextDouble());
		double flow = sensorState.getFlowRate()
				+ (FLOW_VOLATILITY * rand.nextDouble());
		double corrosion = sensorState.getCorrosion()
				+ (rand.nextDouble() / 1000);
		double incline = sensorState.getIncline()
				+ (rand.nextDouble() / 1_000_000);

		SensorReading reading = new SensorReading(id, sensorState.getSegment(),
				System.currentTimeMillis(), sensorState.getLat(),
				sensorState.getLng(), pressure, temp, flow, corrosion, incline);
		reading.withOutputFormat(format);
		return reading;
	}

	private void run(final int events, final OutputFormat format,
			final String streamName, final String region) throws Exception {
		AmazonKinesis kinesisClient = new AmazonKinesisClient(
				new DefaultAWSCredentialsProviderChain());
		kinesisClient.setRegion(Region.getRegion(Regions.fromName(region)));
		int count = 0;
		SensorReading r = null;
		do {
			r = nextSensorReading(format);

			try {
				PutRecordRequest req = new PutRecordRequest()
						.withPartitionKey("" + rand.nextLong())
						.withStreamName(streamName)
						.withData(ByteBuffer.wrap(r.toString().getBytes()));
				kinesisClient.putRecord(req);
			} catch (ProvisionedThroughputExceededException e) {
				Thread.sleep(BACKOFF);
			}

			System.out.println(r);
			count++;
		} while (count < events);
	}

	public static void main(String[] args) throws Exception {
		Integer i = Integer.parseInt(args[0]);
		OutputFormat format = OutputFormat.valueOf(args[1]);
		String streamName = args[2];
		String region = args[3];

		new SensorReadingProducer().run(i, format, streamName, region);
	}
}

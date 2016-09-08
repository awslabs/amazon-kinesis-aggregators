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
package com.amazonaws.services.kinesis.io;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.aggregators.AggregateData;
import com.amazonaws.services.kinesis.aggregators.AggregatorType;
import com.amazonaws.services.kinesis.aggregators.InputEvent;
import com.amazonaws.services.kinesis.aggregators.LabelSet;
import com.amazonaws.services.kinesis.aggregators.StreamAggregator;
import com.amazonaws.services.kinesis.aggregators.StreamAggregatorUtils;
import com.amazonaws.services.kinesis.aggregators.exception.InvalidConfigurationException;
import com.amazonaws.services.kinesis.aggregators.exception.SerializationException;
import com.amazonaws.services.kinesis.aggregators.exception.UnsupportedCalculationException;
import com.amazonaws.services.kinesis.aggregators.summary.SummaryConfiguration;
import com.amazonaws.services.kinesis.io.serializer.JsonSerializer;
import com.fasterxml.jackson.databind.JsonNode;

public class JsonDataExtractor extends AbstractDataExtractor implements
		IDataExtractor {
	private List<String> labelAttributes;

	private String labelName, dateFormat, uniqueIdAttribute,
			dateValueAttribute;

	private SimpleDateFormat dateFormatter;

	private List<String> summaryAttributes;

	private final Log LOG = LogFactory.getLog(JsonDataExtractor.class);

	private Map<String, Double> sumUpdates = new HashMap<>();

	private JsonSerializer serialiser = new JsonSerializer();

	private JsonDataExtractor() {
	}

	public JsonDataExtractor(List<String> labelAttributes) {
		this.labelAttributes = labelAttributes;
		this.labelName = LabelSet.fromStringKeys(labelAttributes).getName();
	}

	public JsonDataExtractor(List<String> labelAttributes,
			JsonSerializer serialiser) {
		this(labelAttributes);
		this.serialiser = serialiser;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<AggregateData> getData(InputEvent event)
			throws SerializationException {
		try {
			List<AggregateData> aggregateData = new ArrayList<>();
			Date dateValue = null;
			JsonNode jsonContent = null;
			String dateString, summary = null;
			sumUpdates = new HashMap<>();

			List<String> items = (List<String>) serialiser.toClass(event);

			// log a warning if we didn't get anything back from the serialiser
			// - this could be OK, but probably isn't
			if (items == null || items.size() == 0)
				LOG.warn(String
						.format("Failed to deserialise any content for Record (Partition Key %s, Sequence %s",
								event.getPartitionKey(),
								event.getSequenceNumber()));

			// process all the items returned by the serialiser
			for (String item : items) {
				// Convert the string to a Jackson JsonNode for navigation
				jsonContent = StreamAggregatorUtils.asJsonNode(item);

				LabelSet labels = new LabelSet();
				for (String key : this.labelAttributes) {
					labels.put(key, StreamAggregatorUtils.readValueAsString(
							jsonContent, key));
				}

				// get the unique ID for the event
				String uniqueId = null;
				if (this.uniqueIdAttribute != null) {
					switch (this.uniqueIdAttribute) {
					case StreamAggregator.REF_PARTITION_KEY:
						uniqueId = event.getPartitionKey();
						break;
					case StreamAggregator.REF_SEQUENCE:
						uniqueId = event.getSequenceNumber();
						break;
					default:
						uniqueId = StreamAggregatorUtils.readValueAsString(
								jsonContent, uniqueIdAttribute);
						break;
					}
				}

				// get the date value from the line
				if (dateValueAttribute != null) {
					dateString = StreamAggregatorUtils.readValueAsString(
							jsonContent, dateValueAttribute);

					// bail on no date returned
					if (dateString == null || dateString.equals(""))
						throw new SerializationException(
								String.format(
										"Unable to read date value attribute %s from JSON Content %s",
										dateValueAttribute, item));

					// turn date as long or string into Date
					if (this.dateFormat != null) {
						dateValue = dateFormatter.parse(dateString);
					} else {
						// no formatter, so treat as epoch seconds
						try {
							dateValue = new Date(Long.parseLong(dateString));
						} catch (Exception e) {
							LOG.error(String
									.format("Unable to create Date Value element from item '%s' due to invalid format as Epoch Seconds",
											dateValueAttribute));
							throw new SerializationException(e);
						}
					}
				} else {
					// no date value attribute configured, so use now
					dateValue = new Date(System.currentTimeMillis());
				}

				// get the summed values
				if (this.aggregatorType.equals(AggregatorType.SUM)) {
					// get the positional sum items
					for (String s : summaryConfig.getItemSet()) {
						try {
							summary = StreamAggregatorUtils.readValueAsString(
									jsonContent, s);

							// if a summary is not found in the data element,
							// then we simply continue without it
							// StreamAggregatorUtils.readValueAsString returns
							// "" if
							// attribute is not found.
							if (summary != null && !summary.equals("")) {
								sumUpdates.put(s, Double.parseDouble(summary));
							}
						} catch (NumberFormatException nfe) {
							LOG.error(String
									.format("Unable to deserialise Summary '%s' due to NumberFormatException",
											s));
							throw new SerializationException(nfe);
						}
					}
				}

				aggregateData.add(new AggregateData(uniqueId, labels,
						dateValue, sumUpdates));
			}

			return aggregateData;
		} catch (Exception e) {
			throw new SerializationException(e);
		}
	}

	/** Builder method to add the attribute which is the event unique id */
	public JsonDataExtractor withUniqueIdAttribute(String uniqueIdAttribute) {
		this.uniqueIdAttribute = uniqueIdAttribute;

		return this;
	}

	/**
	 * Builder Method to add the attribute name which contains the date value
	 * for the stream item.
	 * 
	 * @param dateValueAttribute
	 *            The attribute name which contains the date item.
	 * @return
	 */
	public JsonDataExtractor withDateValueAttribute(String dateValueAttribute) {
		if (dateValueAttribute != null)
			this.dateValueAttribute = dateValueAttribute;
		return this;
	}

	/**
	 * Builder method which allows adding a date format string which can be used
	 * to convert the data value in the dateValueAttribute, if the value is a
	 * string.
	 * 
	 * @param dateFormat
	 *            Date Format in {@link java.text.SimpleDateFormat} form.
	 * @return
	 */
	public JsonDataExtractor withDateFormat(String dateFormat) {
		if (dateFormat != null && !dateFormat.equals("")) {
			this.dateFormat = dateFormat;
			this.dateFormatter = new SimpleDateFormat(dateFormat);
		}
		return this;
	}

	public JsonDataExtractor withSerialiser(JsonSerializer serialiser) {
		this.serialiser = serialiser;
		return this;
	}

	/**
	 * Builder method which allows for setting a list of summary attribute names
	 * or expressions on the data extractor.
	 * 
	 * @param summaryAttributes
	 *            List of summary attribute names or expressions which should be
	 *            extracted from the data
	 * @return
	 * @throws UnsupportedCalculationException
	 */
	public JsonDataExtractor withSummaryAttributes(
			List<String> summaryAttributes)
			throws UnsupportedCalculationException {
		if (summaryAttributes != null) {
			this.aggregatorType = AggregatorType.SUM;
			this.summaryAttributes = summaryAttributes;
			this.summaryConfig = new SummaryConfiguration(summaryAttributes);
		}
		return this;
	}

	/**
	 * Add a regular expression filter to this data extractor. When configured,
	 * only string values which match the regular expression will be
	 * deserialised and have data extracted from it.
	 * 
	 * @param filterRegex
	 *            Regular expression which must match in order for data to be
	 *            subject to data extraction.
	 * @return
	 */
	public JsonDataExtractor withRegexFilter(String filterRegex) {
		if (filterRegex != null) {
			this.serialiser.withFilterRegex(filterRegex);
		}
		return this;
	}

	/**
	 * Add a non default item terminator. The default is "\n"
	 * 
	 * @param lineTerminator
	 *            The characters used for delimiting lines of text
	 * @return
	 */
	public JsonDataExtractor withItemTerminator(String lineTerminator) {
		if (lineTerminator != null) {
			this.serialiser.withItemTerminator(lineTerminator);
		}
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getAggregateLabelName() {
		return this.labelName;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDateValueName() {
		return this.dateValueAttribute == null ? StreamAggregator.DEFAULT_DATE_VALUE
				: this.dateValueAttribute;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getUniqueIdName() {
		return this.uniqueIdAttribute;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void validate() throws Exception {
		if (this.serialiser == null) {
			throw new InvalidConfigurationException(
					"Cannot create instance of JsonDataExtractor without an IKinesisSerialiser");
		}

		if (this.aggregatorType.equals(AggregatorType.SUM)
				&& this.summaryAttributes == null) {
			throw new InvalidConfigurationException(
					"Summary aggregators require configuration of Summary Attributes");
		}
	}

	public IDataExtractor copy() throws Exception {
		return new JsonDataExtractor(this.labelAttributes, this.serialiser)
				.withDateFormat(this.dateFormat)
				.withDateValueAttribute(this.dateValueAttribute)
				.withSummaryAttributes(this.summaryAttributes);
	}
}

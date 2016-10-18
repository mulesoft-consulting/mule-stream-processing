package org.mule.modules.complexeventprocessing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.mule.api.MuleMessage;

import scala.Tuple3;

public class StreamFilterFunction implements  FilterFunction<org.apache.flink.api.java.tuple.Tuple3<String, MuleMessage, Date>> {

	protected static Log logger = LogFactory.getLog(StreamFilterFunction.class);

	final List<String> streams;
		
	public StreamFilterFunction(String[]  names) {
		super();
		this.streams = Arrays.asList(names);
	}

	//@Override
	public boolean filter(Tuple3<String, Object,Date> event) throws Exception {
		logger.info("Filtering on  key: " + event._1() + " for stream: " + streams);		
		return streams.contains(event._1());
	}

	@Override
	public boolean filter(org.apache.flink.api.java.tuple.Tuple3<String, MuleMessage, Date> event) throws Exception {
		logger.info("Filtering on  key: " + event.f0 + " for stream: " + streams);		
		return streams.contains(event.f0);
	}

}

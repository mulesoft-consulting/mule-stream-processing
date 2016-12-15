package org.mule.modules.complexeventprocessing;

import java.util.ArrayList;
import java.util.Arrays;
import java.sql.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.mule.api.MuleMessage;

public class StreamFilterFunction implements FilterFunction<Tuple3<String, MuleMessage, Date>> {

	protected static Log logger = LogFactory.getLog(StreamFilterFunction.class);

	final List<String> streams;

	String filterExpression;

	public StreamFilterFunction(String[] names, String filterExpression) {
		super();
		this.streams = Arrays.asList(names);
		this.filterExpression = filterExpression;
	}

	@Override
	public boolean filter(Tuple3<String, MuleMessage, Date> event) throws Exception {
		logger.info("Filtering on  key: " + event.f0 + " for stream: " + streams);	
		if (filterExpression == null) {
			return streams.contains(event.f0);
		} else {
			if (streams.contains(event.f0)) {
				return MuleStreamProcessing.muleContext.getExpressionManager()
						.evaluateBoolean(filterExpression, event.f1);
								
			} else {
				return false;
			}
		}
	}

}

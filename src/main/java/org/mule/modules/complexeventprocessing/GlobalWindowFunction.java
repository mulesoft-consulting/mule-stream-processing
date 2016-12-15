package org.mule.modules.complexeventprocessing;

import java.util.ArrayList;
import java.util.Arrays;
import java.sql.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.mule.DefaultMessageCollection;
import org.mule.api.MuleMessage;

public class GlobalWindowFunction implements WindowFunction<Tuple3<String, MuleMessage, Date>, MuleMessage, String, GlobalWindow> {

	protected static Log logger = LogFactory.getLog(StreamWindowFunction.class);
	
	final List<String> streams;
    String filterExpression;
    
	public GlobalWindowFunction(String[]  names, String filterExpression) {
		super();
		this.streams = Arrays.asList(names);
		this.filterExpression = filterExpression;
	}

	@Override
	public void apply(String key, GlobalWindow window, Iterable<Tuple3<String, MuleMessage, Date>> input,
			Collector<MuleMessage> out) throws Exception {
		List<MuleMessage> events = new ArrayList<>();
		for (Tuple3<String, MuleMessage,Date> tuple : input) {
			logger.info("Processing event in window: " + tuple.f1.getMessageRootId() + " for stream: " + tuple.f0);
			logger.info("Filter expression: " + filterExpression);
			if (streams.contains(tuple.f0)) {
				if (filterExpression != null) {
					if (MuleStreamProcessing.muleContext.getExpressionManager().evaluateBoolean(filterExpression, tuple.f1)) {
						events.add(tuple.f1);
					}
				} else {
					logger.info("Adding event to tuple");
					events.add(tuple.f1);
				}
			}
		}
		DefaultMessageCollection collection = 
				new DefaultMessageCollection(MuleStreamProcessing.muleContext);
		collection.addMessages(events);
		out.collect(collection);		
		
	}

}

package org.mule.modules.complexeventprocessing;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.mule.api.MuleMessage;

public class StreamNameKeySelector implements KeySelector<Tuple3<String, MuleMessage, Date>, String> {

	protected static Log logger = LogFactory.getLog(StreamNameKeySelector.class);

	@Override
	public String getKey(Tuple3<String, MuleMessage, Date> event) throws Exception {
		logger.info("Returning key: " + event.f0);
		return event.f0;
	}

}

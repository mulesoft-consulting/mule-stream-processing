package org.mule.modules.complexeventprocessing;

import java.sql.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.mule.api.MuleMessage;

import com.mycila.event.Topics;

public class ObjectSource extends RichSourceFunction<Tuple3<String, MuleMessage, Date>> {

	protected static Log logger = LogFactory.getLog(ObjectSource.class);

	volatile boolean isRunning = true;

	final String topic;

	public ObjectSource(String topic) {
		super();
		this.topic = topic;
	}

	@Override
	public void run(final SourceFunction.SourceContext<Tuple3<String, MuleMessage, Date>> ctx) throws Exception {
		logger.info("Running source function");

		if (topic.equals("*")) {
			ComplexEventProcessingConnector.dispatcher.subscribe(Topics.any(),
					Tuple3.class, new EventSubscriber(ctx));
		} else {
			ComplexEventProcessingConnector.dispatcher.subscribe(Topics.match(topic), 
					Tuple3.class, new EventSubscriber(ctx));
		}
		while (isRunning) {
			Thread.sleep(100L);
		}
		logger.info("Done consuming off queue");
	}

	@Override
	public void cancel() {
		isRunning = false;
	}

}

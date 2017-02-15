package org.mule.modules.complexeventprocessing;

import java.sql.Date;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.mule.api.MuleMessage;

import com.mycila.event.Event;
import com.mycila.event.Subscriber;

public class EventSubscriber implements Subscriber<Tuple3<String,MuleMessage,Date>>{
	
	protected static Log logger = LogFactory.getLog(EventSubscriber.class);

	final SourceFunction.SourceContext<Tuple3<String, MuleMessage, Date>> ctx;
	
    final String id;
    
	public EventSubscriber(SourceContext<Tuple3<String, MuleMessage, Date>> ctx) {
		super();
		id = UUID.randomUUID().toString();
		logger.info("Subscribing to event stream: " + id);
		this.ctx = ctx;
	}

	@Override
	public void onEvent(Event<Tuple3<String, MuleMessage, Date>> tuple) throws Exception {	
		logger.info(id + " received event off bus: " + tuple.getSource().f1.getMessageRootId());
		
		ctx.collectWithTimestamp(tuple.getSource(), tuple.getSource().f2.getTime());
		
	}

}

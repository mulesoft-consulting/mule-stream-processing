package org.mule.modules.complexeventprocessing;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.mule.api.MuleMessage;
import org.mule.api.callback.SourceCallback;

public class SourceCallbackSinkFunction implements SinkFunction<MuleMessage>{
	
	protected static Log logger = LogFactory.getLog(SourceCallbackSinkFunction.class);

	final String sourceCallback;	

	public SourceCallbackSinkFunction(String sourceCallback) {
		super();
		this.sourceCallback = sourceCallback;
	}

	@Override
	public void invoke(MuleMessage value) throws Exception {
		logger.info("Sending event: " + value.getMessageRootId());
		// ToDo Ugly as sin workaround because SourceCallbacks aren't Serializable
		ComplexEventProcessingConnector.callbackMap.get(sourceCallback).process(value);
	}


}

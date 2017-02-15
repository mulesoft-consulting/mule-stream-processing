package org.mule.modules.complexeventprocessing;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

public class SourceCallbackRowSinkFunction implements SinkFunction<Row>{
	
	protected static Log logger = LogFactory.getLog(SourceCallbackSinkFunction.class);

	final String sourceCallback;

	public SourceCallbackRowSinkFunction(String sourceCallback) {
		super();
		this.sourceCallback = sourceCallback;
	}

	@Override
	public void invoke(Row row) throws Exception {

		List<Object> result = new ArrayList<>();
		for (int i=0; i < row.getArity(); i++) {
			result.add(row.getField(i));
		}
		if (result.size() > 0) {
			MuleStreamProcessing.callbackMap.get(sourceCallback).process(result);
		}
	}

}

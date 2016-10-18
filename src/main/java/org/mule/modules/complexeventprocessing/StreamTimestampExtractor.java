package org.mule.modules.complexeventprocessing;

import java.util.Date;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.mule.api.MuleMessage;

public class StreamTimestampExtractor implements AssignerWithPeriodicWatermarks<Tuple3<String, MuleMessage, Date>> {

	@Override
	public long extractTimestamp(Tuple3<String, MuleMessage, Date> element, long previousElementTimestamp) {
		// TODO Auto-generated method stub
		return element.f2.getTime();
	}

	@Override
	public Watermark getCurrentWatermark() {
		// TODO Auto-generated method stub
		return null;
	}

}

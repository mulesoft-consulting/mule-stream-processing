package org.mule.modules.complexeventprocessing;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.mule.api.MuleContext;
import org.mule.api.MuleMessage;
import org.mule.api.annotations.Config;
import org.mule.api.annotations.Connector;
import org.mule.api.annotations.Processor;
import org.mule.api.annotations.Source;
import org.mule.api.callback.SourceCallback;
import org.mule.api.context.MuleContextAware;
import org.mule.extension.annotations.param.Optional;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.mule.modules.complexeventprocessing.config.ConnectorConfig;
import org.mule.util.UUID;

import com.mycila.event.Dispatcher;
import com.mycila.event.Dispatchers;
import com.mycila.event.Topic;

@Connector(name = "complex-event-processing", friendlyName = "ComplexEventProcessing")
public class ComplexEventProcessingConnector implements MuleContextAware {

	protected transient Log logger = LogFactory.getLog(getClass());

	static Map<String, SourceCallback> callbackMap = new HashMap<>();

	static Dispatcher dispatcher;

	@Inject
	static MuleContext muleContext;

	@Config
	ConnectorConfig config;

	private StreamExecutionEnvironment executionEnvironment;

	private boolean started = false;
	

	@PostConstruct
	public void initialize() throws Exception {
		logger.info("Initializing Flink");

		executionEnvironment = StreamExecutionEnvironment.createLocalEnvironment();
		executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		executionEnvironment.addDefaultKryoSerializer(org.mule.api.MuleMessage.class, 
				MuleMessageSerializer.class);
		
		dispatcher = Dispatchers.asynchronousSafe();	
	}

	
	public void startExecutionEnvironment() throws Exception {
		logger.info("Starting Flink execution environment in background thread");
		Executors.newSingleThreadExecutor().execute(new Runnable() {
			@Override
			public void run() {
				try {
					executionEnvironment.execute();
				} catch (Throwable t) {
					logger.error("Error", t);
				} finally {
					logger.info("Stopping execution environment");
				}
			}
		});
	}

	@Processor
	public void send(String stream, MuleMessage message) throws Exception {
		synchronized (this) {
			if (!started) {
				startExecutionEnvironment();
				started = true;
			}
		}

		logger.info("Broadcasting event " + message.getMessageRootId() + " to stream: " + stream);
		dispatcher.publish(Topic.topic("events"), Tuple3.of(stream, message, new Date()));
	}

	@Source
	public synchronized void listen(String streams, @Optional TimeUnit timeUnit, 
			@Optional Long interval, @Optional String filterExpression, final SourceCallback callback) {
		logger.info("Registering window for streams: " + streams);

		String callbackId = UUID.getUUID().toString();
		callbackMap.put(callbackId, callback);

		logger.info("Registering event source for stream: " + streams);

		DataStream<Tuple3<String, MuleMessage, Date>> dataStream = executionEnvironment.addSource(new ObjectSource(),
				callbackId);
		dataStream.assignTimestampsAndWatermarks(new StreamTimestampExtractor());

		KeyedStream<Tuple3<String, MuleMessage, Date>, String> keyedEvents = 
				dataStream.keyBy(new StreamNameKeySelector());
		if (interval != null) {
			keyedEvents.window(TumblingEventTimeWindows.of(Time.of(interval, timeUnit))).apply(
					new StreamWindowFunction(streams.split(","), filterExpression))
					.addSink(new SourceCallbackSinkFunction(callbackId));
		} else {
			
			// keyedEvents.filter((new StreamWindowFunction(streams.split(","))
		}
	}

	public ConnectorConfig getConfig() {
		return config;
	}

	public void setConfig(ConnectorConfig config) {
		this.config = config;
	}

	@Override
	public void setMuleContext(MuleContext muleContext) {
		this.muleContext = muleContext;

	}
}
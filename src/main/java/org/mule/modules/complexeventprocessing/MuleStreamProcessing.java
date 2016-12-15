package org.mule.modules.complexeventprocessing;

import java.sql.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.java.table.StreamTableEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.table.Table;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.mule.api.MuleContext;
import org.mule.api.MuleMessage;
import org.mule.api.annotations.Config;
import org.mule.api.annotations.Connector;
import org.mule.api.annotations.Processor;
import org.mule.api.annotations.Source;
import org.mule.api.annotations.param.Optional;
import org.mule.api.callback.SourceCallback;
import org.mule.api.context.MuleContextAware;
import org.mule.modules.complexeventprocessing.config.ConnectorConfig;
import org.mule.util.UUID;

import com.mycila.event.Dispatcher;
import com.mycila.event.Dispatchers;
import com.mycila.event.Topic;

@Connector(name = "mule-stream-processing", friendlyName = "MuleStreamProcessing")
public class MuleStreamProcessing implements MuleContextAware {

	protected transient Log logger = LogFactory.getLog(getClass());

	static Map<String, SourceCallback> callbackMap = new HashMap<>();

	static Dispatcher dispatcher;

	@Inject
	static MuleContext muleContext;

	@Config
	ConnectorConfig config;

	private StreamExecutionEnvironment executionEnvironment;
	private StreamTableEnvironment tableEnvironment;

	private boolean started = false;

	@PostConstruct
	public void initialize() throws Exception {
		logger.info("Initializing Flink");

		executionEnvironment = StreamExecutionEnvironment.createLocalEnvironment();
		executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		executionEnvironment.addDefaultKryoSerializer(org.mule.api.MuleMessage.class, MuleMessageSerializer.class);

		tableEnvironment = StreamTableEnvironment.getTableEnvironment(executionEnvironment);

		dispatcher = Dispatchers.asynchronousSafe();
	}

	public void startExecutionEnvironment() throws Exception {
		logger.info("Starting Flink execution environment in background thread");
		final CountDownLatch latch = new CountDownLatch(1);
		Executors.newSingleThreadExecutor().execute(new Runnable() {
			@Override
			public void run() {
				try {
					latch.countDown();
					executionEnvironment.execute();
				} catch (Throwable t) {
					logger.error("Error", t);
				} finally {
					logger.info("Stopping execution environment");
				}
			}
		});
		latch.await(1, TimeUnit.MINUTES);
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
		dispatcher.publish(Topic.topic(stream), Tuple3.of(stream, message,
				new Date(new java.util.Date().getTime())));
	}

	@Source
	public synchronized void query(String query, String streams, final SourceCallback callback) 
			throws Exception {
		String callbackId = UUID.getUUID().toString();

		callbackMap.put(callbackId, callback);

		for (String stream : streams.split(",")) {
			
			DataStream<Tuple3<String, MuleMessage, Date>> dataStream = executionEnvironment
					.addSource(new ObjectSource(stream), callbackId);
			Table in = tableEnvironment.fromDataStream(dataStream, "id,message,timestamp");
			tableEnvironment.registerTable(stream, in);
			tableEnvironment.registerFunction("MEL", new ExpressionFunction());
		}
		
		Table result = tableEnvironment.sql(query);
		result.writeToSink(new SourceCallbackTableSink(callbackId));
	}

	@Source
	public synchronized void listen(String streams, @Optional TimeUnit timeUnit, @Optional Long interval,
			@Optional String filterExpression, final SourceCallback callback) {
		logger.info("Registering window for streams: " + streams);

		String callbackId = UUID.getUUID().toString();
		callbackMap.put(callbackId, callback);

		logger.info("Registering event source for stream: " + streams);

		// ToDo 911 fix this, i don't think we need a wildcard here and subsequent key filter on Flink
		DataStream<Tuple3<String, MuleMessage, Date>> dataStream = executionEnvironment.addSource(new ObjectSource("*"),
				callbackId);
		dataStream.assignTimestampsAndWatermarks(new StreamTimestampExtractor());

		KeyedStream<Tuple3<String, MuleMessage, Date>, String> keyedEvents = dataStream
				.keyBy(new StreamNameKeySelector());
		if (interval != null) {
			keyedEvents.window(TumblingEventTimeWindows.of(Time.of(interval, timeUnit)))
					.apply(new StreamWindowFunction(streams.split(","), filterExpression))
					.addSink(new SourceCallbackSinkFunction(callbackId));
		} else {
			keyedEvents.window(GlobalWindows.create())
					.trigger(PurgingTrigger.of(ContinuousProcessingTimeTrigger.of(Time.seconds(1))))
					.apply(new GlobalWindowFunction(streams.split(","), filterExpression))
					.addSink(new SourceCallbackSinkFunction(callbackId));
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
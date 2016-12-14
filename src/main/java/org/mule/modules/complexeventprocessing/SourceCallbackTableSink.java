package org.mule.modules.complexeventprocessing;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.sinks.StreamTableSink;
import org.apache.flink.api.table.sinks.TableSink;
import org.apache.flink.api.table.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;


public class SourceCallbackTableSink  implements StreamTableSink<Row> {

	final String sourceCallback;
	
	protected String[] fieldNames;
    protected TypeInformation[] fieldTypes;


	public SourceCallbackTableSink(String sourceCallback) {
		super();
		this.sourceCallback = sourceCallback;
	}

	@Override
	public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		// TODO Auto-generated method stub
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
		return this;
//	
//		SourceCallbackTableSink copy = new SourceCallbackTableSink(sourceCallback);
//		copy.configure(fieldNames, fieldTypes);
//		return copy;
	}

	@Override
	public String[] getFieldNames() {
        return fieldNames;
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
	}

	@Override
	public TypeInformation<Row> getOutputType() {
		// TODO Auto-generated method stub
        return new RowTypeInfo(getFieldTypes());
	}

	@Override
	public void emitDataStream(DataStream<Row> stream) {
		stream.addSink(new SourceCallbackRowSinkFunction(sourceCallback));
	}

	
	

	
	// 		stream.addSink(new SourceCallbackSinkFunction(sourceCallback));


}

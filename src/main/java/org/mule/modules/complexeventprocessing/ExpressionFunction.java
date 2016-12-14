package org.mule.modules.complexeventprocessing;

import org.apache.flink.api.table.functions.ScalarFunction;
import org.mule.api.MuleMessage;

public class ExpressionFunction extends ScalarFunction {
	public Object eval(String expression, MuleMessage message) {
		return 
    			ComplexEventProcessingConnector.muleContext
    			.getExpressionManager().evaluate(expression, message);
		
	}
}

package org.mule.modules.complexeventprocessing;

import org.apache.flink.table.functions.ScalarFunction;
import org.mule.api.MuleMessage;

public class ExpressionFunction extends ScalarFunction {
	public Object eval(String expression, MuleMessage message) {
		return 
    			MuleStreamProcessing.muleContext
    			.getExpressionManager().evaluate(expression, message);
		
	}
}

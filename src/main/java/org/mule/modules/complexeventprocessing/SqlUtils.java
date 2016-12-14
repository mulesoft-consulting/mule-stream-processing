package org.mule.modules.complexeventprocessing;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlVisitor;

public class SqlUtils {

	public static List<String> valuesFromSelect(String query) throws Exception {

		final List<String> values = new ArrayList<>();

		SqlNode node = SqlParser.create(query).parseQuery();

		node.accept(new SqlVisitor<Object>() {
			@Override
			public Object visit(SqlLiteral sqlLiteral) {
				return null;
			}

			@Override
			public Object visit(SqlCall sqlCall) {
				SqlSelect select = (SqlSelect) sqlCall;
				for (SqlNode node : select.getSelectList()) {
					values.add(node.toString());
				}
				return sqlCall;
			}

			@Override
			public Object visit(SqlNodeList sqlNodeList) {
				return null;
			}

			@Override
			public Object visit(SqlIdentifier sqlIdentifier) {
				return null;
			}

			@Override
			public Object visit(SqlDataTypeSpec sqlDataTypeSpec) {
				return null;
			}

			@Override
			public Object visit(SqlDynamicParam sqlDynamicParam) {
				return null;
			}

			@Override
			public Object visit(SqlIntervalQualifier sqlIntervalQualifier) {
				return null;
			}
		});
		
		return values;
	}

}

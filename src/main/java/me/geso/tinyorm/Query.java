package me.geso.tinyorm;

import java.util.List;

/**
 *
 * @author Tokuhiro Matsuno
 */
public class Query {
	@Override
	public String toString() {
		return "Query [sql=" + sql + ", values=" + values + "]";
	}

	private final String sql;
	private final List<Object> values;

	public Query(String sql, List<Object> values) {
		this.sql = sql;
		this.values = values;
	}

	public String getSQL() {
		return sql;
	}

	public Object[] getValues() {
		return values.toArray();
	}
}

package me.geso.tinyorm;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.dbutils.QueryRunner;

/**
 * UPDATE statement for one row.
 * 
 * @author Tokuhiro Matsuno <tokuhirom@gmail.com>
 */
public class UpdateRowStatement {

	private final Row row;
	private final Map<String, Object> set = new TreeMap<>();
	private boolean executed = false;

	UpdateRowStatement(Row row) {
		this.row = row;
	}

	public UpdateRowStatement set(String column, Object value) {
		this.set.put(column, value);
		return this;
	}

	public UpdateRowStatement setByBean(Object bean) {
		try {
			Map<String, String> describe = BeanUtils.describe(bean);
			describe.remove("class");
			this.set.putAll(describe);
			return this;
		} catch (IllegalAccessException | InvocationTargetException
				| NoSuchMethodException e) {
			throw new RuntimeException(e);
		}
	}

	public void execute() {
		Query where = row.where();
		StringBuilder buf = new StringBuilder();
		buf.append("UPDATE ").append(this.row.getTableName()).append(" SET ");
		set.keySet().stream()
				.map(col -> col + "=?")
				.collect(
						Collectors.collectingAndThen(Collectors.joining(","),
								it -> buf.append(it)));
		buf.append(" WHERE ").append(where.getSQL());
		String sql = buf.toString();
		ArrayList<Object> values = new ArrayList<>();
		values.addAll(set.values());
		for (Object o : where.getValues()) {
			values.add(o);
		}

		this.executed = true;

		try {
			new QueryRunner()
					.update(row.getConnection(), sql, values.toArray());
		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Suppress warnings at finalize().
	 */
	public void discard() {
		this.executed = true;
	}

	protected void finalize() throws Throwable {
		if (!this.executed) {
			throw new RuntimeException(
					"You may forgot to call 'execute' method on UpdateRowStatement.");
		}
	}
}

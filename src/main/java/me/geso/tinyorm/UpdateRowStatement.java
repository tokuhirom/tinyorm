package me.geso.tinyorm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;


/**
 * UPDATE statement for one row.
 * 
 * @author Tokuhiro Matsuno
 */
public class UpdateRowStatement {

	@Override
	public String toString() {
		return "UpdateRowStatement [row=" + row + ", set=" + set
				+ ", executed=" + executed + ", connection=" + connection
				+ ", tableName=" + tableName + "]";
	}

	private final Row row;
	private final Map<String, Object> set = new TreeMap<>();
	private boolean executed = false;
	private final Connection connection;
	private final String tableName;

	UpdateRowStatement(Row row, Connection connection, String tableName) {
		this.row = row;
		this.connection = connection;
		this.tableName = tableName;
	}

	public UpdateRowStatement set(String column, Object value) {
		this.set.put(column, value);
		return this;
	}

	public void execute() {
		Query where = row.where();
		StringBuilder buf = new StringBuilder();
		buf.append("UPDATE ").append(this.tableName).append(" SET ");
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
			PreparedStatement stmt = TinyORM.prepare(connection, sql, values.toArray());
			stmt.executeUpdate();
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

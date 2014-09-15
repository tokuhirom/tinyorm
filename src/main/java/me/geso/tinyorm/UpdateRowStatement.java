package me.geso.tinyorm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import lombok.ToString;
import me.geso.tinyorm.meta.TableMeta;

/**
 * UPDATE statement for one row.
 * 
 * @author Tokuhiro Matsuno
 */
@ToString
public class UpdateRowStatement {

	private final Row row;
	private final Map<String, Object> set = new TreeMap<>();
	private boolean executed = false;
	private final Connection connection;
	private final TableMeta tableMeta;

	UpdateRowStatement(Row row, Connection connection, TableMeta tableMeta) {
		this.row = row;
		this.connection = connection;
		this.tableMeta = tableMeta;
	}

	public UpdateRowStatement set(String column, Object value) {
		if (column == null) {
			throw new IllegalArgumentException("Column name must not be null");
		}
		this.set.put(column, value);
		return this;
	}

	/**
	 * Should I call execute() method?
	 * 
	 * @return
	 */
	public boolean hasSetClause() {
		return !this.set.isEmpty();
	}

	public void execute() {
		this.tableMeta.invokeBeforeUpdateTriggers(this);
		String tableName = tableMeta.getName();

		Query where = row.where();
		String whereSQL = where.getSQL();
		if (whereSQL.isEmpty()) {
			throw new RuntimeException("Empty where clause");
		}
		StringBuilder buf = new StringBuilder();
		buf.append("UPDATE ").append(tableName).append(" SET ");
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

		try (PreparedStatement preparedStatement = connection
				.prepareStatement(sql)) {
			TinyORMUtil.fillPreparedStatementParams(preparedStatement,
					values.toArray());
			preparedStatement.executeUpdate();
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

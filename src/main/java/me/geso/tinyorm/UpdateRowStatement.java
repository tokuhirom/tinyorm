package me.geso.tinyorm;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * UPDATE statement for one row.
 * 
 * @author Tokuhiro Matsuno
 */
@ToString
@Slf4j
public class UpdateRowStatement {

	private final Object row;
	private final Map<String, Object> set = new TreeMap<>();
	private boolean executed = false;
	private final Connection connection;
	private final TableMeta tableMeta;

	UpdateRowStatement(Object row, Connection connection, TableMeta tableMeta) {
		this.row = row;
		this.connection = connection;
		this.tableMeta = tableMeta;
	}

	public UpdateRowStatement set(String columnName, Object value) {
		if (columnName == null) {
			throw new IllegalArgumentException("Column name must not be null");
		}
		if (value != null) {
			value = this.tableMeta.invokeDeflater(columnName, value);
		}
		this.set.put(columnName, value);
		return this;
	}

	public UpdateRowStatement setBean(Object bean) {
		if (!this.set.isEmpty()) {
			throw new RuntimeException(
					"You can't call setBean() method the UpdateRowStatement has SET clause information.");
		}
		Map<String, Object> currentValueMap = tableMeta
				.getColumnValueMap(this.row);

		try {
			BeanInfo beanInfo = Introspector.getBeanInfo(bean.getClass(),
					Object.class);
			PropertyDescriptor[] propertyDescriptors = beanInfo
					.getPropertyDescriptors();
			for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
				String name = propertyDescriptor.getName();
				if (!currentValueMap.containsKey(name)) {
					// Ignore values doesn't exists in Row bean.
					continue;
				}

				Object current = currentValueMap.get(name);
				Object newval = propertyDescriptor.getReadMethod().invoke(bean);
				if (newval != null) {
					if (!newval.equals(current)) {
						this.set(name, newval);
					}
				} else { // newval IS NULL.
					if (current != null) {
						this.set(name, null);
					}
				}
			}
		} catch (IllegalAccessException | IllegalArgumentException
				| InvocationTargetException | IntrospectionException e) {
			throw new RuntimeException(e);
		}

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
		if (!this.hasSetClause()) {
			if (log.isDebugEnabled()) {
				log.debug("There is no modification");
			}
			return; // There is no updates.
		}

		this.tableMeta.invokeBeforeUpdateTriggers(this);
		String tableName = tableMeta.getName();

		Query where = tableMeta.createWhereClauseFromRow(row, connection);
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

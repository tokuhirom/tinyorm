package me.geso.tinyorm;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import me.geso.jdbcutils.JDBCUtils;
import me.geso.jdbcutils.Query;
import me.geso.jdbcutils.QueryBuilder;
import me.geso.jdbcutils.RichSQLException;

/**
 * UPDATE statement for one row.
 * 
 * @author Tokuhiro Matsuno
 */
@ToString
@Slf4j
public class UpdateRowStatement<T extends Row<?>> {

	private final Object row;
	private final Map<String, Object> set = new TreeMap<>();
	private boolean executed = false;
	private final Connection connection;
	private final TableMeta<T> tableMeta;
	private final String identifierQuoteString;

	UpdateRowStatement(T row, Connection connection, TableMeta<T> tableMeta,
			final String identifierQuoteString) {
		this.row = row;
		this.connection = connection;
		this.tableMeta = tableMeta;
		this.identifierQuoteString = identifierQuoteString;
	}

	public UpdateRowStatement<T> set(String columnName, Object value) {
		if (columnName == null) {
			throw new IllegalArgumentException("Column name must not be null");
		}
		if (value != null) {
			value = this.tableMeta.invokeDeflater(columnName, value);
		}
		if (!tableMeta.hasColumn(columnName)) {
			throw new IllegalArgumentException(columnName
					+ " is not listed in " + tableMeta.getName()
					+ " column list.");
		}
		Object current = tableMeta.getValue(this.row, columnName);
		if (ObjectUtils._equals(current, value)) {
			// We don't need to update database. do nothing.
		} else {
			this.set.put(columnName, value);
		}
		return this;
	}

	public UpdateRowStatement<T> setBean(Object bean) {
		if (!this.set.isEmpty()) {
			throw new RuntimeException(
					"You can't call setBean() method the UpdateRowStatement has SET clause information.");
		}

		try {
			BeanInfo beanInfo = Introspector.getBeanInfo(bean.getClass(),
					Object.class);
			PropertyDescriptor[] propertyDescriptors = beanInfo
					.getPropertyDescriptors();
			for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
				String name = propertyDescriptor.getName();
				if (!tableMeta.hasColumn(name)) {
					// Ignore values doesn't exists in Row bean.
					continue;
				}
				if (propertyDescriptor.getReadMethod() == null) {
					continue;
				}

				Object value = propertyDescriptor.getReadMethod().invoke(bean);
				this.set(name, value);
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

	public void execute() throws RichSQLException {
		if (!this.hasSetClause()) {
			if (log.isDebugEnabled()) {
				log.debug("There is no modification");
			}
			return; // There is no updates.
		}

		this.tableMeta.invokeBeforeUpdateTriggers(this);
		String tableName = tableMeta.getName();

		final Query where = tableMeta.createWhereClauseFromRow(row,
				this.identifierQuoteString);
		if (where.getSQL().isEmpty()) {
			throw new RuntimeException("Empty where clause");
		}
		Query query = new QueryBuilder(this.identifierQuoteString)
				.appendQuery("UPDATE ")
				.appendIdentifier(tableName)
				.appendQuery(" SET ")
				.appendQuery(
						set.keySet()
								.stream()
								.map(col -> JDBCUtils.quoteIdentifier(col,
										identifierQuoteString) + "=?")
								.collect(
										Collectors.joining(",")))
				.addParameters(set.values())
				.appendQuery(" WHERE ")
				.append(where)
				.build();

		this.executed = true;

		JDBCUtils.executeUpdate(connection, query);
	}

	/**
	 * Suppress warnings at finalize().
	 */
	public void discard() {
		this.executed = true;
	}

	protected void finalize() throws Throwable {
		if (!this.executed && this.hasSetClause()) {
			throw new RuntimeException(
					"You may forgot to call 'execute' method on UpdateRowStatement.");
		}
	}
}

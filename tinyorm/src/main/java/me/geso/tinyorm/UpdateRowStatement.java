package me.geso.tinyorm;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import me.geso.jdbcutils.JDBCUtils;
import me.geso.jdbcutils.Query;
import me.geso.jdbcutils.QueryBuilder;
import me.geso.jdbcutils.UncheckedRichSQLException;

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
	private final TableMeta<T> tableMeta;
	private final String identifierQuoteString;
	private final TinyORM orm;
	private boolean executed = false;

	UpdateRowStatement(T row, TinyORM orm) {
		this.row = row;
		this.orm = orm;
		this.tableMeta = orm.getTableMeta((Class<T>)row.getClass());
		this.identifierQuoteString = orm.getIdentifierQuoteString();
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
		// If there is no modification, do not send update query to database.
		if (!Objects.equals(current, value)) {
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
			BeanInfo beanInfo = Introspector.getBeanInfo(bean.getClass(), Object.class);
			PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
			for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
				Method readMethod = propertyDescriptor.getReadMethod();
				if (readMethod == null) {
					continue;
				}

				Object value = readMethod.invoke(bean);
				tableMeta.getColumnName(propertyDescriptor)
					.ifPresent(columnName -> this.set(columnName, value));
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
	 * @return true if the statement object has set clause, false otherwise.
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

		final String sql = query.getSQL();
		final List<Object> params = query.getParameters();
		try (final PreparedStatement ps = orm.prepareStatement(sql)) {
			JDBCUtils.fillPreparedStatementParams(ps, params);
			ps.executeUpdate();
		} catch (final SQLException ex) {
			throw new UncheckedRichSQLException(ex, sql, params);
		}
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
		super.finalize();
	}
}

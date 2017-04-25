/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.geso.tinyorm;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import me.geso.jdbcutils.JDBCUtils;
import me.geso.jdbcutils.Query;
import me.geso.jdbcutils.QueryBuilder;
import me.geso.jdbcutils.UncheckedRichSQLException;

/**
 *
 * @author Tokuhiro Matsuno
 * @param <T>
 */
public class InsertStatement<T extends Row<?>> {
	// it should be ordered.
	private final Map<String, Object> values = new LinkedHashMap<>();
	private final Class<T> klass;
	private final TinyORM orm;
	private final TableMeta<T> tableMeta;
	private List<String> onDuplicateKeyUpdateQuery;
	private List<Object> onDuplicateKeyUpdateValues;

	InsertStatement(TinyORM orm, Class<T> klass, TableMeta<T> tableMeta) {
		if (orm == null) {
			throw new RuntimeException("orm should not be null");
		}
		this.orm = orm;
		this.klass = klass;
		this.tableMeta = tableMeta;
	}

	public Class<T> getRowClass() {
		return this.klass;
	}

	/**
	 * Add new value.
	 * 
	 * @param columnName Column name to set value.
	 * @param value New value for the column
	 * @return Object itself.
	 */
	public InsertStatement<T> value(String columnName, Object value) {
		Object deflated = this.tableMeta.invokeDeflater(columnName, value);
		values.put(columnName, deflated);
		return this;
	}

	/**
	 * Set values by map.
	 *
	 * @param values values to set
	 * @return InsertStatement object itself.
	 */
	public InsertStatement<T> value(Map<String, Object> values) {
		values.keySet().stream()
			.forEach(it -> this.value(it, values.get(it)));
		return this;
	}

	/**
	 * Set values by Bean.
	 * 
	 * @param valueBean bean object
	 * @return InsertStatement object itself for fluent interface.
	 */
	public InsertStatement<T> valueByBean(Object valueBean) {
		try {
			BeanInfo beanInfo = Introspector.getBeanInfo(valueBean.getClass(), Object.class);
			for (PropertyDescriptor propertyDescriptor : beanInfo.getPropertyDescriptors()) {
				Method readMethod = propertyDescriptor.getReadMethod();
				if (readMethod == null) {
					continue;
				}

				Object value = readMethod.invoke(valueBean);
				tableMeta.getColumnName(propertyDescriptor)
					.ifPresent(columnName -> this.value(columnName, value));
			}
			return this;
		} catch (IntrospectionException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * [EXPERIMENTAL] Add "ON DUPLICATE KEY UPDATE " clause.
	 *
	 * <code>
	 *     insert.set("name", "John")
	 *     		 .onDuplicateKeyUpdate("name=?", "John")
	 * </code>
	 *
	 * @param query Query in string.
	 * @return InsertStatement object itself for fluent interface.
	 */
	public InsertStatement<T> onDuplicateKeyUpdate(String query,
			Object... params) {
		if (onDuplicateKeyUpdateQuery == null) {
			onDuplicateKeyUpdateQuery = new ArrayList<>();
		}
		onDuplicateKeyUpdateQuery.add(query);

		if (onDuplicateKeyUpdateValues == null) {
			onDuplicateKeyUpdateValues = new ArrayList<>();
		}
		Collections.addAll(onDuplicateKeyUpdateValues, params);
		return this;
	}

	private Query buildQuery() {
		final String identifierQuoteString = orm.getIdentifierQuoteString();
		final QueryBuilder builder = new QueryBuilder(orm.getConnection())
			.appendQuery("INSERT INTO ")
			.appendIdentifier(tableMeta.getName())
			.appendQuery(" (")
			.appendQuery(
				values.keySet()
					.stream()
					.map(key -> JDBCUtils.quoteIdentifier(key,
						identifierQuoteString))
					.collect(Collectors.joining(",")))
			.appendQuery(") VALUES (")
			.appendQuery(values.values().stream().map(e -> "?")
				.collect(Collectors.joining(",")))
			.addParameters(values.values())
			.appendQuery(")");
		if (onDuplicateKeyUpdateQuery != null) {
			builder.appendQuery(" ON DUPLICATE KEY UPDATE ");
			builder.appendQuery(
					onDuplicateKeyUpdateQuery.stream()
						.collect(Collectors.joining(",")));
			builder.addParameters(onDuplicateKeyUpdateValues);
		}
		return builder.build();
	}

	/**
	 * Execute query.
	 * 
	 * @return The number of affected rows.
	 */
	public int execute() {
		this.tableMeta.invokeBeforeInsertTriggers(this);
		final Query query = this.buildQuery();

		final String sql = query.getSQL();
		final List<Object> params = query.getParameters();
		try (final PreparedStatement ps = orm.getConnection().prepareStatement(sql)) {
			JDBCUtils.fillPreparedStatementParams(ps, params);
			final int inserted = ps.executeUpdate();
			if (inserted != 1 && this.onDuplicateKeyUpdateQuery == null) {
				throw new RuntimeException("Cannot insert to database:"
						+ query);
			}
			return inserted;
		} catch (final SQLException ex) {
			throw new UncheckedRichSQLException(ex, sql, params);
		}
	}

	public T executeSelect() {
		try {
			this.execute();

			final List<PropertyDescriptor> primaryKeys = this.tableMeta
				.getPrimaryKeys();
			final String tableName = this.tableMeta.getName();
			if (primaryKeys.isEmpty()) {
				throw new RuntimeException(
					"You can't call InsertStatement#executeSelect() on the table doesn't have a primary keys.");
			}
			if (primaryKeys.size() > 1) {
				throw new RuntimeException(
					"You can't call InsertStatement#executeSelect() on the table has multiple primary keys.");
			}
			final String pkName = primaryKeys.get(0).getName();

			final Connection connection = this.orm.getConnection();
			final Query query = new QueryBuilder(connection)
				.appendQuery("SELECT * FROM ")
				.appendIdentifier(tableName)
				.appendQuery(" WHERE ")
				.appendIdentifier(pkName)
				.appendQuery("=last_insert_id()")
				.build();
			final Optional<T> maybeRow = this.orm.singleBySQL(klass, query, connection);
			if (maybeRow.isPresent()) {
				return maybeRow.get();
			} else {
				throw new RuntimeException(
					"Cannot get the row after insertion: " + tableName);
			}
		} catch (SecurityException ex) {
			throw new RuntimeException(ex);
		}
	}
}

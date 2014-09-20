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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Tokuhiro Matsuno
 * @param <T>
 */
public class InsertStatement<T> {
	private static final Logger logger = LoggerFactory
			.getLogger(InsertStatement.class);

	// it should be ordered.
	private final Map<String, Object> values = new LinkedHashMap<>();
	private final Class<T> klass;
	private final TinyORM orm;
	private final TableMeta tableMeta;
	private final Map<String, Object> onDuplicateKeyUpdate = new LinkedHashMap<>();

	InsertStatement(TinyORM orm, Class<T> klass, TableMeta tableMeta) {
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
	 * @param columnName
	 * @param value
	 * @return
	 */
	public InsertStatement<T> value(String columnName, Object value) {
		try {
			Object deflated = this.tableMeta.invokeDeflater(columnName, value);
			values.put(columnName, deflated);
			return this;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public InsertStatement<T> value(Map<String, Object> values) {
		values.keySet().stream()
				.forEach(it -> {
					this.value(it, values.get(it));
				});
		return this;
	}

	/**
	 * Set values by Bean.
	 * 
	 * @param valueBean
	 * @return
	 */
	public InsertStatement<T> valueByBean(Object valueBean) {
		try {
			BeanInfo beanInfo = Introspector.getBeanInfo(valueBean.getClass(),
					Object.class);
			for (PropertyDescriptor propertyDescriptor : beanInfo
					.getPropertyDescriptors()) {
				Method readMethod = propertyDescriptor.getReadMethod();
				if (readMethod != null) {
					Object value = readMethod.invoke(valueBean);
					this.value(propertyDescriptor.getName(), value);
				}
			}
			return this;
		} catch (IntrospectionException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException e) {
			throw new RuntimeException(e);
		}
	}

	public String buildSQL() {
		StringBuilder buf = new StringBuilder();
		buf.append("INSERT INTO ").append(tableMeta.getName()).append(" (");
		buf.append(values.keySet().stream().collect(Collectors.joining(",")));
		buf.append(") VALUES (");
		buf.append(values.values().stream().map(e -> "?")
				.collect(Collectors.joining(",")));
		buf.append(")");
		if (!onDuplicateKeyUpdate.isEmpty()) {
			buf.append(" ON DUPLICATE KEY UPDATE ");
			String piece = onDuplicateKeyUpdate.keySet().stream()
					.map(column -> column + "=?")
					.collect(Collectors.joining(","))
			; // TODO quote identifier
			buf.append(piece);
		}
		String sql = buf.toString();
		return sql;
	}
	
	public Object[] buildValues() {
		List<Object> params = new ArrayList<>(values.values());
		params.addAll(onDuplicateKeyUpdate.values());
		return params.toArray();
	}

	public void execute() {
		this.tableMeta.invokeBeforeInsertTriggers(this);
		String sql = buildSQL();
		Object[] params = this.buildValues();

		try {
			try (PreparedStatement preparedStatement = orm.getConnection()
					.prepareStatement(sql)) {
				TinyORMUtil.fillPreparedStatementParams(preparedStatement,
						params);
				int inserted = preparedStatement.executeUpdate();
				if (inserted != 1) {
					throw new RuntimeException("Cannot insert to database:"
							+ sql);
				}
			}
		} catch (SQLException ex) {
			logger.error("SQLException: {} {} {}", ex.getMessage(), sql,
					params.toString());
			throw new RuntimeException(ex);
		}
	}

	public T executeSelect() {
		try {
			this.execute();

			List<PropertyDescriptor> primaryKeyMetas = this.tableMeta
					.getPrimaryKeys();
			String tableName = this.tableMeta.getName();
			if (primaryKeyMetas.isEmpty()) {
				throw new RuntimeException(
						"You can't call InsertStatement#executeSelect() on the table doesn't have a primary keys.");
			}
			if (primaryKeyMetas.size() > 1) {
				throw new RuntimeException(
						"You can't call InsertStatement#executeSelect() on the table has multiple primary keys.");
			}
			String pkName = primaryKeyMetas.get(0).getName();

			Connection connection = this.orm.getConnection();
			String sql = "SELECT * FROM "
					+ TinyORMUtil.quoteIdentifier(tableName, connection)
					+ " WHERE "
					+ TinyORMUtil.quoteIdentifier(pkName, connection)
					+ "=last_insert_id()";
			Optional<T> maybeRow = this.orm.singleBySQL(klass, sql,
					new Object[] {});
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

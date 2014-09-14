/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.geso.tinyorm;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.SneakyThrows;
import me.geso.tinyorm.meta.PrimaryKeyMeta;
import me.geso.tinyorm.meta.TableMeta;

/**
 *
 * @author Tokuhiro Matsuno
 * @param <T>
 */
public class InsertStatement<T extends Row> {

	// it should be ordered.
	private final Map<String, Object> values = new LinkedHashMap<>();
	private final Class<T> klass;
	private final TinyORM orm;
	private final TableMeta tableMeta;

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
	 * @param column
	 * @param value
	 * @return
	 */
	public InsertStatement<T> value(String columnName, Object value) {
		try {
			Object deflated = this.tableMeta.invokeDeflaters(columnName, value);
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
	@SneakyThrows
	public InsertStatement<T> valueByBean(Object valueBean) {
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
	}

	public String buildSQL() {
		StringBuilder buf = new StringBuilder();
		buf.append("INSERT INTO ").append(tableMeta.getName()).append(" (");
		buf.append(values.keySet().stream().collect(Collectors.joining(",")));
		buf.append(") VALUES (");
		buf.append(values.values().stream().map(e -> "?")
				.collect(Collectors.joining(",")));
		buf.append(")");
		String sql = buf.toString();
		return sql;
	}

	public void execute() {
		try {
			this.tableMeta.invokeBeforeInsertTriggers(this);
			String sql = buildSQL();
			int inserted = TinyORM.prepare(orm.getConnection(), sql,
					values.values().toArray()).executeUpdate();
			if (inserted != 1) {
				throw new RuntimeException("Cannot insert to database:" + sql);
			}
		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}

	public T executeSelect() {
		try {
			this.execute();

			List<PrimaryKeyMeta> primaryKeyMetas = this.tableMeta
					.getPrimaryKeyMetas();
			String tableName = this.tableMeta.getName();
			if (primaryKeyMetas.isEmpty()) {
				throw new RuntimeException(
						"You can't call InsertStatement#executeSelect() on the table doesn't have a primary keys.");
			}
			if (primaryKeyMetas.size() > 1) {
				throw new RuntimeException(
						"You can't call InsertStatement#executeSelect() on the table has multiple primary keys.");
			}

			Connection connection = this.orm.getConnection();
			String sql = "SELECT * FROM "
					+ TinyORM.quoteIdentifier(tableName, connection)
					+ " WHERE "
					+ TinyORM.quoteIdentifier(primaryKeyMetas.get(0).getName(),
							connection)
					+ "=last_insert_id()";
			Optional<T> maybeRow = this.orm.single(klass, sql);
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

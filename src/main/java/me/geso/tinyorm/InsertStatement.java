/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.geso.tinyorm;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.commons.beanutils.BeanUtilsBean;

/**
 *
 * @author Tokuhiro Matsuno
 * @param <T>
 */
public class InsertStatement<T extends Row> {

	private final String table;

	// it should be ordered.
	private final Map<String, Object> values = new TreeMap<>();
	private final Class<T> klass;
	private final TinyORM orm;

	InsertStatement(TinyORM orm, Class<T> klass) {
		if (orm == null) {
			throw new RuntimeException("orm should not be null");
		}
		this.orm = orm;
		this.klass = klass;
		this.table = TinyORM.getTableName(klass);
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
	public InsertStatement<T> value(String column, Object value) {
		try {
			Method method = this.klass.getMethod("DEFLATE", String.class,
					Object.class);
			Object deflated = method.invoke(this.klass, column, value);
			values.put(column, deflated);
			return this;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Set values by Bean.
	 * 
	 * @param valueBean
	 * @return
	 */
	public InsertStatement<T> valueByBean(Object valueBean) {
		try {
			Map<String, Object> describe = BeanUtilsBean.getInstance().getPropertyUtils().describe(valueBean);
			describe.keySet().stream().filter(it -> !"class".equals(it))
					.forEach(it -> {
						this.value(it, describe.get(it));
					});
			return this;
		} catch (IllegalAccessException | InvocationTargetException
				| NoSuchMethodException e) {
			throw new RuntimeException(e);
		}
	}

	public String buildSQL() {
		StringBuilder buf = new StringBuilder();
		buf.append("INSERT INTO ").append(table).append(" (");
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
			this.orm.BEFORE_INSERT(this);
			String sql = buildSQL();
			int inserted = TinyORM.prepare(orm.getConnection(), sql, values.values().toArray()).executeUpdate();
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

			List<String> primaryKeys = TinyORM.getPrimaryKeys(klass);
			if (primaryKeys.isEmpty()) {
				throw new RuntimeException(
						"You can't call InsertStatement#executeSelect() on the table doesn't have a primary keys.");
			}
			if (primaryKeys.size() > 1) {
				throw new RuntimeException(
						"You can't call InsertStatement#executeSelect() on the table has multiple primary keys.");
			}

			Connection connection = this.orm.getConnection();
			String sql = "SELECT * FROM "
					+ TinyORM.quoteIdentifier(table, connection)
					+ " WHERE "
					+ TinyORM.quoteIdentifier(primaryKeys.get(0), connection)
					+ "=last_insert_id()";
			Optional<T> maybeRow = this.orm.single(klass, sql);
			if (maybeRow.isPresent()) {
				return maybeRow.get();
			} else {
				throw new RuntimeException(
						"Cannot get the row after insertion: " + table);
			}
		} catch (SecurityException ex) {
			throw new RuntimeException(ex);
		}
	}
}

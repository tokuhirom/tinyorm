/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.geso.tinyorm;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;

/**
 *
 * @author Tokuhiro Matsuno <tokuhirom@gmail.com>
 * @param <T>
 */
public class InsertStatement<T extends Row> {

	private final String table;

	// it should be ordered.
	private final Map<String, Object> values = new TreeMap<>();
	private final Connection conn;
	private final Class<T> klass;

	InsertStatement(Connection conn, Class<T> klass) {
		this.conn = conn;
		this.klass = klass;
		this.table = TinyORM.getTableName(klass);
	}

	/**
	 * Add new value.
	 * 
	 * @param column
	 * @param value
	 * @return
	 */
	public InsertStatement<T> value(String column, Object value) {
		values.put(column, value);
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
			Map<String, String> describe = BeanUtils.describe(valueBean);
			describe.keySet().stream().filter(it -> !"class".equals(it))
					.forEach(it -> {
						values.put(it, describe.get(it));
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
			String sql = buildSQL();
			int inserted = new QueryRunner().update(conn, sql, values.values()
					.toArray());
			if (inserted != 1) {
				throw new RuntimeException("Cannot insert to database:" + sql);
			}
		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}

	public T executeSelect() {
		try {
			execute();
			List<String> primaryKeys = TinyORM.getPrimaryKeys(klass);
			if (primaryKeys.isEmpty()) {
				throw new RuntimeException(
						"You can't call InsertStatement#executeSelect() on the table doesn't have a primary keys.");
			}
			if (primaryKeys.size() > 1) {
				throw new RuntimeException(
						"You can't call InsertStatement#executeSelect() on the table has multiple primary keys.");
			}
			T row = new QueryRunner().query(conn, "SELECT * FROM " + table
					+ " WHERE " + primaryKeys.get(0) + "=last_insert_id()",
					new BeanHandler<>(klass));
			row.setConnection(conn);
			return row;
		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}
}

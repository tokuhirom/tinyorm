/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.geso.tinyorm;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

/**
 * Tiny O/R Mapper implementation.
 * 
 * @author Tokuhiro Matsuno <tokuhirom@gmail.com>
 */
public abstract class TinyORM {

	public abstract Connection getConnection();

	public <T extends Row> InsertStatement<T> insert(Class<T> klass) {
		return new InsertStatement<>(this, klass);
	}

	/**
	 * User can override this method for hooking. For example, you can set the
	 * "created_on" value at here.
	 * 
	 * @param insert
	 */
	public <T extends Row> void BEFORE_INSERT(InsertStatement<T> insert) {
		Field[] fields = insert.getRowClass().getFields();
		for (Field field : fields) {
			if ("updatedOn".equals(field.getName())) {
				insert.value(field.getName(), System.currentTimeMillis() / 1000);
			}
			if ("createdOn".equals(field.getName())) {
				insert.value(field.getName(), System.currentTimeMillis() / 1000);
			}
		}
	}

	/**
	 * Get table name from the row class.
	 * 
	 * @param klass
	 * @return
	 */
	public static String getTableName(Class<? extends Row> klass) {
		Table table = klass.getAnnotation(Table.class);
		if (table != null) {
			return table.value();
		} else {
			throw new RuntimeException("Missing @Table annotation: " + klass);
		}
	}

	/**
	 * Get primary keys from the row class.
	 * 
	 * @param class1
	 * @return
	 */
	public static List<String> getPrimaryKeys(Class<? extends Row> class1) {
		List<String> primaryKeys = new ArrayList<>();
		Field[] fields = class1.getDeclaredFields();
		for (Field field : fields) {
			if (field.getAnnotation(PrimaryKey.class) != null) {
				primaryKeys.add(field.getName());
			}
		}
		return primaryKeys;
	}

	/**
	 * Execute an SELECT statement via dbutils.
	 * 
	 * @param sql
	 * @param rh
	 *            dbutil's ResultSetHandler
	 * @param params
	 * @return
	 */
	public <T> T query(String sql, ResultSetHandler<T> rh, Object... params) {
		try {
			return new QueryRunner().<T> query(this.getConnection(), sql, rh,
					params);
		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Select one row from the database.
	 */
	public <T extends Row> Optional<T> single(Class<T> klass, String sql,
			Object... params) {
		try {
			Connection connection = this.getConnection();
			ResultSet rs = TinyORM.prepare(connection, sql, params).executeQuery();
			if (rs.next()) {
				T row = TinyORM.mapResultSet(klass, rs, connection);
				return Optional.of(row);
			} else {
				return Optional.empty();
			}
		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}

	static PreparedStatement prepare(Connection connection, String sql,
			Object... params) {
		try {
			PreparedStatement stmt = connection.prepareStatement(sql);
			for (int i = 0; i < params.length; ++i) {
				stmt.setObject(i + 1, params[i]);
			}
			return stmt;
		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Create new <code>BeanSelectStatement</code> for selecting 1 row.
	 * 
	 * @param klass
	 *            Target entity class.
	 * @return
	 */
	public <T extends Row> BeanSelectStatement<T> single(Class<T> klass) {
		return new BeanSelectStatement<>(this.getConnection(),
				TinyORM.getTableName(klass), klass);
	}

	/**
	 * Create new <code>ListSelectStatement</code> for selecting rows.
	 * 
	 * @param klass
	 *            Target entity class.
	 * @return
	 */
	public <T extends Row> ListSelectStatement<T> search(Class<T> klass) {
		return new ListSelectStatement<>(this.getConnection(),
				TinyORM.getTableName(klass), klass);
	}

	/**
	 * Create new <code>PaginatedSelectStatement</code> for selecting rows.
	 * 
	 * @param klass
	 * @return
	 */
	public <T extends Row> PaginatedSelectStatement<T> searchWithPager(
			Class<T> klass) {
		return new PaginatedSelectStatement<>(this.getConnection(),
				TinyORM.getTableName(klass), klass);
	}

	/**
	 * Select multiple rows from the database.
	 */
	public <T extends Row> List<T> search(Class<T> klass, String sql,
			Object... params) {
		try {
			Connection connection = this.getConnection();
			ResultSet rs = TinyORM.prepare(connection, sql, params).executeQuery();
			List<T> list = new ArrayList<>();
			while (rs.next()) {
				T row = TinyORM.mapResultSet(klass, rs, connection);
				list.add(row);
			}
			return list;
		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Execute an UPDATE, INSERT, and DELETE query.
	 */
	public int update(String sql, Object... params) {
		try {
			return new QueryRunner().update(this.getConnection(), sql, params);
		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Quote SQL identifier. You should get identifierQuoteString from
	 * DatabaseMetadata.
	 *
	 * @param identifier
	 * @param identifierQuoteString
	 * @return Escaped identifier.
	 */
	public static String quoteIdentifier(String identifier,
			String identifierQuoteString) {
		return identifierQuoteString
				+ identifier.replace(identifierQuoteString,
						identifierQuoteString + identifierQuoteString)
				+ identifierQuoteString;
	}

	/**
	 * Quote SQL indentifier.
	 * 
	 * @param identifier
	 * @param connection
	 * @return
	 */
	public static String quoteIdentifier(String identifier,
			Connection connection) {
		if (connection == null) {
			throw new NullPointerException();
		}
		try {
			String identifierQuoteString = connection.getMetaData()
					.getIdentifierQuoteString();
			return quoteIdentifier(identifier, identifierQuoteString);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	// I should make public. But I don't like this method name.
	// I need your suggestion.
	static <T extends Row> T mapResultSet(Class<T> klass, ResultSet rs,
			Connection connection) {
		try {
			int columnCount = rs.getMetaData().getColumnCount();
			Method INFLATE = klass.getMethod("INFLATE",
					String.class, Object.class);
			T row = klass.newInstance();
			for (int i = 0; i < columnCount; ++i) {
				String column = rs.getMetaData().getColumnName(i + 1);
				Object value = rs.getObject(i + 1);
				value = INFLATE.invoke(klass, column, value);
				BeanUtils.setProperty(row, column, value);
			}
			row.setConnection(connection);
			return row;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.geso.tinyorm;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;

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
	 * User can override this method for hooking.
	 * For example, you can set the "created_on" value at here.
	 * 
	 * @param insert
	 */
	public <T extends Row> void BEFORE_INSERT(InsertStatement<T> insert) {
		// Do nothing.

		/*
		// Here is a example implementation.

		try {
			if (insert.getRowClass().getField("created_on") != null) {
				insert.value("created_on", System.currentMillis()/1000);
			}
		} catch (NoSuchFieldException | SecurityException e) {
			throw new RuntimeException(e);
		}
		*/
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
	public <T> Optional<T> single(Class<T> klass, String sql, Object... params) {
		try {
			T row = new QueryRunner().query(this.getConnection(), sql,
					new BeanHandler<>(klass),
					params);
			return Optional.ofNullable(row);
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
	 * Select multiple rows from the database.
	 */
	public <T> List<T> search(Class<T> klass, String sql, Object... params) {
		try {
			List<T> list = new QueryRunner().query(this.getConnection(), sql,
					new BeanListHandler<>(klass),
					params);
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

}

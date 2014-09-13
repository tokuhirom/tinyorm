/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.geso.tinyorm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import me.geso.tinyorm.meta.TableMeta;
import me.geso.tinyorm.meta.TableMetaRepository;

/**
 * Tiny O/R Mapper implementation.
 * 
 * @author Tokuhiro Matsuno
 */
public abstract class TinyORM {

	public abstract Connection getConnection();

	public <T extends Row> InsertStatement<T> insert(Class<T> klass) {
		return new InsertStatement<>(this, klass);
	}

	/**
	 * Select one row from the database.
	 */
	public <T extends Row> Optional<T> single(Class<T> klass, String sql,
			Object... params) {
		try {
			Connection connection = this.getConnection();
			ResultSet rs = TinyORM.prepare(connection, sql, params)
					.executeQuery();
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
		String tableName = TableMetaRepository.get(klass).getName();
		return new BeanSelectStatement<>(this.getConnection(),
				tableName, klass);
	}

	/**
	 * Create new <code>ListSelectStatement</code> for selecting rows.
	 * 
	 * @param klass
	 *            Target entity class.
	 * @return
	 */
	public <T extends Row> ListSelectStatement<T> search(Class<T> klass) {
		String tableName = TableMetaRepository.get(klass).getName();
		return new ListSelectStatement<>(this.getConnection(),
				tableName, klass);
	}

	/**
	 * Create new <code>PaginatedSelectStatement</code> for selecting rows.
	 * 
	 * @param klass
	 * @return
	 */
	public <T extends Row> PaginatedSelectStatement<T> searchWithPager(
			Class<T> klass) {
		String tableName = TableMetaRepository.get(klass).getName();
		return new PaginatedSelectStatement<>(this.getConnection(),
				tableName, klass);
	}

	/**
	 * Select multiple rows from the database.
	 */
	public <T extends Row> List<T> search(Class<T> klass, String sql,
			Object... params) {
		try {
			Connection connection = this.getConnection();
			ResultSet rs = TinyORM.prepare(connection, sql, params)
					.executeQuery();
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
			PreparedStatement stmt = TinyORM.prepare(this.getConnection(), sql,
					params);
			return stmt.executeUpdate();
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
		TableMeta tableMeta = TableMetaRepository.get(klass);
		try {
			int columnCount = rs.getMetaData().getColumnCount();
			T row = klass.newInstance();
			for (int i = 0; i < columnCount; ++i) {
				String columnName = rs.getMetaData().getColumnName(i + 1);
				Object value = rs.getObject(i + 1);
				value = tableMeta.invokeInflaters(columnName, value);
				tableMeta.setValue(row, columnName, value);
			}
			row.setConnection(connection);
			return row;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Select single long value
	 * 
	 * @return
	 */
	public OptionalLong selectLong(String sql, Object... params) {
		try {
			PreparedStatement stmt = TinyORM.prepare(this.getConnection(), sql,
					params);
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				long l = rs.getLong(1);
				return OptionalLong.of(l);
			} else {
				return OptionalLong.empty();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

}

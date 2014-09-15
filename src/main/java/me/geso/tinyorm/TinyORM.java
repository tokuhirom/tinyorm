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

import me.geso.tinyorm.meta.DBSchema;
import me.geso.tinyorm.meta.TableMeta;

/**
 * Tiny O/R Mapper implementation.
 * 
 * @author Tokuhiro Matsuno
 */
public abstract class TinyORM {

	public abstract Connection getConnection();

	public abstract DBSchema getSchema();

	public <T extends Row> InsertStatement<T> insert(Class<T> klass) {
		return new InsertStatement<>(this, klass, this.getTableMeta(klass));
	}

	/**
	 * Select one row from the database.
	 */
	public <T extends Row> Optional<T> single(Class<T> klass, String sql,
			Object... params) {
		try {
			Connection connection = this.getConnection();
			try (PreparedStatement preparedStatement = connection
					.prepareStatement(sql)) {
				TinyORMUtil.fillPreparedStatementParams(preparedStatement,
						params);
				try (ResultSet rs = preparedStatement.executeQuery()) {
					TableMeta tableMeta = this.getTableMeta(klass);
					if (rs.next()) {
						T row = TinyORM.mapResultSet(klass, rs, connection,
								tableMeta);
						return Optional.of(row);
					} else {
						return Optional.empty();
					}
				}
			}
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
		TableMeta tableMeta = this.getTableMeta(klass);
		return new BeanSelectStatement<>(this.getConnection(),
				klass, tableMeta);
	}

	/**
	 * Create new <code>ListSelectStatement</code> for selecting rows.
	 * 
	 * @param klass
	 *            Target entity class.
	 * @return
	 */
	public <T extends Row> ListSelectStatement<T> search(Class<T> klass) {
		TableMeta tableMeta = this.getTableMeta(klass);
		return new ListSelectStatement<>(this.getConnection(),
				klass, tableMeta);
	}

	/**
	 * Create new <code>PaginatedSelectStatement</code> for selecting rows.
	 * 
	 * @param klass
	 * @return
	 */
	public <T extends Row> PaginatedSelectStatement<T> searchWithPager(
			Class<T> klass) {
		TableMeta tableMeta = this.getTableMeta(klass);
		return new PaginatedSelectStatement<>(this.getConnection(),
				klass, tableMeta);
	}

	/**
	 * Select multiple rows from the database.
	 */
	public <T extends Row> List<T> search(Class<T> klass, String sql,
			Object... params) {
		try {
			Connection connection = this.getConnection();
			try (PreparedStatement preparedStatement = connection
					.prepareStatement(sql)) {
				TinyORMUtil.fillPreparedStatementParams(preparedStatement,
						params);
				try (ResultSet rs = preparedStatement.executeQuery()) {
					List<T> list = new ArrayList<>();
					TableMeta tableMeta = this.getTableMeta(klass);
					while (rs.next()) {
						T row = TinyORM.mapResultSet(klass, rs, connection,
								tableMeta);
						list.add(row);
					}
					return list;
				}
			}
		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Execute an UPDATE, INSERT, and DELETE query.
	 */
	public int update(String sql, Object... params) {
		try (PreparedStatement preparedStatement = this.getConnection()
				.prepareStatement(sql)) {
			TinyORMUtil.fillPreparedStatementParams(preparedStatement, params);
			return preparedStatement.executeUpdate();
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

	static <T extends Row> T mapResultSet(Class<T> klass, ResultSet rs,
			Connection connection, TableMeta tableMeta) {
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
			row.setTableMeta(tableMeta);
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
		try (PreparedStatement preparedStatement = this.getConnection()
				.prepareStatement(sql)) {
			TinyORMUtil.fillPreparedStatementParams(preparedStatement, params);
			try (ResultSet rs = preparedStatement.executeQuery()) {
				if (rs.next()) {
					long l = rs.getLong(1);
					return OptionalLong.of(l);
				} else {
					return OptionalLong.empty();
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private TableMeta getTableMeta(Class<? extends Row> klass) {
		return this.getSchema().getTableMeta(klass);
	}

}

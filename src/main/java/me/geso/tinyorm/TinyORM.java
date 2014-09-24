/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.geso.tinyorm;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import me.geso.jdbcutils.JDBCUtils;
import me.geso.jdbcutils.Query;
import me.geso.jdbcutils.QueryBuilder;
import me.geso.jdbcutils.ResultSetCallback;
import me.geso.jdbcutils.RichSQLException;

/**
 * Tiny O/R Mapper implementation.
 * 
 * @author Tokuhiro Matsuno
 */
@Slf4j
public class TinyORM {

	private final Connection connection;

	private static ConcurrentHashMap<Class<?>, TableMeta<?>> tableMetaRegistry = new ConcurrentHashMap<>();

	public TinyORM(Connection connection) {
		this.connection = connection;
	}

	public Connection getConnection() {
		return this.connection;
	}

	/**
	 * Create {@code InsertStatement} for sending INSERT statement.<br>
	 * {@code orm.insert(Member.class)
	 * 	.value("name", "John")
	 * 	.execute();
	 * }
	 * 
	 * @param klass
	 * @return
	 */
	public <T extends Row<?>> InsertStatement<T> insert(Class<T> klass) {
		@SuppressWarnings("unchecked")
		TableMeta<T> tableMeta = (TableMeta<T>) this.getTableMeta(klass);
		return new InsertStatement<>(this, klass, tableMeta);
	}

	/**
	 * Select one row from the database.
	 * 
	 * @throws RichSQLException
	 */
	public <T extends Row<?>> Optional<T> singleBySQL(Class<T> klass,
			String sql,
			List<Object> params) throws RichSQLException {
		@SuppressWarnings("unchecked")
		TableMeta<T> tableMeta = (TableMeta<T>) this.getTableMeta(klass);

		return JDBCUtils.executeQuery(
				connection,
				sql,
				params,
				(rs) -> {
					if (rs.next()) {
						final T row = tableMeta.createRowFromResultSet(klass,
								rs, this);
						return Optional.of(row);
					} else {
						return Optional.empty();
					}
				});
	}

	/**
	 * Select one row from the database.
	 * 
	 * @param klass
	 * @param query
	 * @return
	 * @throws RichSQLException
	 */
	public <T extends Row<?>> Optional<T> singleBySQL(Class<T> klass,
			Query query)
			throws RichSQLException {
		return this.singleBySQL(klass, query.getSQL(), query.getParameters());
	}

	/**
	 * Create new <code>BeanSelectStatement</code> for selecting 1 row.
	 * 
	 * @param klass
	 *            Target entity class.
	 * @return
	 */
	public <T extends Row<?>> BeanSelectStatement<T> single(Class<T> klass) {
		@SuppressWarnings("unchecked")
		TableMeta<T> tableMeta = (TableMeta<T>) this.getTableMeta(klass);
		return new BeanSelectStatement<>(this.getConnection(),
				klass, tableMeta, this);
	}

	/**
	 * Create new <code>ListSelectStatement</code> for selecting rows.
	 * 
	 * @param klass
	 *            Target entity class.
	 * @return
	 */
	public <T extends Row<?>> ListSelectStatement<T> search(Class<T> klass) {
		@SuppressWarnings("unchecked")
		TableMeta<T> tableMeta = (TableMeta<T>) this.getTableMeta(klass);
		return new ListSelectStatement<>(this.getConnection(),
				klass, tableMeta, this);
	}

	/**
	 * Create new <code>PaginatedSelectStatement</code> for selecting rows.
	 * 
	 * @param klass
	 * @return
	 */
	public <T extends Row<?>> PaginatedSelectStatement<T> searchWithPager(
			final Class<T> klass, final long limit) {
		@SuppressWarnings("unchecked")
		TableMeta<T> tableMeta = (TableMeta<T>) this.getTableMeta(klass);
		return new PaginatedSelectStatement<>(this.getConnection(),
				klass, tableMeta, this, limit);
	}

	/**
	 * Search by SQL.
	 * 
	 * @throws RichSQLException
	 * 
	 */
	public <T extends Row<?>> List<T> searchBySQL(
			final Class<T> klass, final String sql, final List<Object> params)
			throws RichSQLException {
		return JDBCUtils.executeQuery(connection, sql, params, (rs) -> {
			List<T> rows = this.mapRowListFromResultSet(klass, rs);
			return rows;
		});
	}

	/**
	 * Search by SQL.
	 * 
	 * @throws RichSQLException
	 * 
	 */
	public <T extends Row<?>> List<T> searchBySQL(final Class<T> klass,
			final String sql)
			throws RichSQLException {
		return this.searchBySQL(klass, sql, Collections.emptyList());
	}

	/**
	 * Search by SQL with Pager.
	 * 
	 * @throws RichSQLException
	 * 
	 */
	public <T extends Row<?>> Paginated<T> searchBySQLWithPager(
			final Class<T> klass, final String sql, final List<Object> params,
			final long entriesPerPage) throws RichSQLException {
		String limitedSql = sql + " LIMIT " + (entriesPerPage + 1);
		Connection connection = this.getConnection();
		return JDBCUtils.executeQuery(connection, limitedSql, params, (rs) -> {
			List<T> rows = this.mapRowListFromResultSet(klass, rs);
			return new Paginated<T>(rows, entriesPerPage);
		});
	}

	<T extends Row<?>> UpdateRowStatement<T> createUpdateStatement(T row) {
		@SuppressWarnings("unchecked")
		TableMeta<T> tableMeta = (TableMeta<T>) this.getTableMeta(row
				.getClass());
		UpdateRowStatement<T> stmt = new UpdateRowStatement<>(row,
				this.getConnection(), tableMeta,
				this.getIdentifierQuoteString());
		return stmt;
	}

	/**
	 * Execute an UPDATE, INSERT, and DELETE query.
	 * 
	 * @throws RichSQLException
	 */
	public int updateBySQL(final String sql, final List<Object> params)
			throws RichSQLException {
		return JDBCUtils.executeUpdate(connection, sql, params);
	}

	/**
	 * Execute an UPDATE, INSERT, and DELETE query.
	 * 
	 * @throws RichSQLException
	 */
	public int updateBySQL(String sql) throws RichSQLException {
		return JDBCUtils.executeUpdate(connection, sql);
	}

	/**
	 * Execute an UPDATE, INSERT, and DELETE query.
	 * 
	 * @throws RichSQLException
	 */
	public int updateBySQL(Query query) throws RichSQLException {
		return JDBCUtils.executeUpdate(connection, query);
	}

	<T extends Row<?>> List<T> mapRowListFromResultSet(Class<T> klass,
			ResultSet rs)
			throws SQLException {
		@SuppressWarnings("unchecked")
		TableMeta<T> tableMeta = (TableMeta<T>) this.getTableMeta(klass);
		ArrayList<T> rows = new ArrayList<>();
		while (rs.next()) {
			T row = tableMeta.createRowFromResultSet(klass, rs, this);
			rows.add(row);
		}
		return rows;
	}

	/**
	 * Shortcut method.
	 * 
	 * @param klass
	 * @param rs
	 * @return
	 * @throws SQLException
	 */
	@Deprecated
	<T extends Row<?>> T mapRowFromResultSet(Class<T> klass, ResultSet rs)
			throws SQLException {
		@SuppressWarnings("unchecked")
		TableMeta<T> tableMeta = (TableMeta<T>) this.getTableMeta(klass);
		return tableMeta.createRowFromResultSet(klass, rs, this);
	}

	/**
	 * Select single long value
	 * 
	 * @return
	 * @throws RichSQLException
	 */
	public OptionalLong queryForLong(final String sql,
			@NonNull final List<Object> params)
			throws RichSQLException {
		return JDBCUtils.executeQuery(connection, sql, params, (rs) -> {
			if (rs.next()) {
				final long l = rs.getLong(1);
				return OptionalLong.of(l);
			} else {
				return OptionalLong.empty();
			}
		});
	}

	/**
	 * Select single long value from database.
	 * 
	 * @return
	 * @throws RichSQLException
	 */
	public OptionalLong queryForLong(@NonNull final String sql)
			throws RichSQLException {
		return this.queryForLong(sql, Collections.emptyList());
	}

	/**
	 * Select single String value from database.
	 * 
	 * @return
	 * @throws RichSQLException
	 */
	public Optional<String> queryForString(final String sql,
			@NonNull final List<Object> params)
			throws RichSQLException {
		return JDBCUtils.executeQuery(connection, sql, params, (rs) -> {
			if (rs.next()) {
				final String s = rs.getString(1);
				return Optional.of(s);
			} else {
				return Optional.empty();
			}
		});
	}

	/**
	 * Select single String value from database without parameters.
	 * 
	 * @return
	 * @throws RichSQLException
	 */
	public Optional<String> queryForString(@NonNull final String sql)
			throws RichSQLException {
		return this.queryForString(sql, Collections.emptyList());
	}

	public <T extends Row<?>> void delete(final T row) throws RichSQLException {
		final Connection connection = this.getConnection();
		@SuppressWarnings("unchecked")
		final TableMeta<T> tableMeta = (TableMeta<T>) this.getTableMeta(row
				.getClass());
		final String tableName = tableMeta.getName();
		final String identifierQuoteString = this.getIdentifierQuoteString();
		final Query where = tableMeta.createWhereClauseFromRow(row,
				identifierQuoteString);

		final Query query = new QueryBuilder(identifierQuoteString)
				.appendQuery("DELETE FROM ")
				.appendIdentifier(tableName)
				.appendQuery(" WHERE ")
				.append(where)
				.build();

		final int updated = JDBCUtils.executeUpdate(connection, query);
		if (updated != 1) {
			throw new RuntimeException("Cannot delete row: " + query);
		}
	}

	@SuppressWarnings("unchecked")
	<T extends Row<?>> Optional<T> refetch(final T row) throws RichSQLException {
		final Connection connection = this.getConnection();
		final TableMeta<T> tableMeta = (TableMeta<T>) this.getTableMeta(row
				.getClass());
		final String identifierQuoteString = this.getIdentifierQuoteString();
		final Query where = tableMeta.createWhereClauseFromRow(row,
				identifierQuoteString);

		final Query query = new QueryBuilder(identifierQuoteString)
				.appendQuery("SELECT * FROM ")
				.appendIdentifier(tableMeta.getName())
				.appendQuery(" WHERE ")
				.append(where)
				.build();

		return JDBCUtils.executeQuery(
				connection,
				query,
				(rs) -> {
					if (rs.next()) {
						final T refetched = tableMeta.createRowFromResultSet(
								(Class<T>) row.getClass(), rs, this);
						return Optional.of(refetched);
					} else {
						return Optional.empty();
					}
				});
	}

	<T extends Row<?>> TableMeta<?> getTableMeta(final Class<T> klass) {
		return tableMetaRegistry.computeIfAbsent(klass, key -> {
			log.info("Loading {}", klass);
			try {
				return TableMeta.<T> build(klass);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
	}

	String getIdentifierQuoteString() {
		try {
			return this.getConnection().getMetaData()
					.getIdentifierQuoteString();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Execute query builder.
	 *
	 * @return
	 */
	public QueryBuilder createQueryBuilder() {
		return new QueryBuilder(this.getIdentifierQuoteString());
	}

	/**
	 * Execute query.
	 * 
	 * @param query
	 * @param callback
	 * @return
	 * @throws RichSQLException
	 */
	public <T> T executeQuery(final Query query,
			final ResultSetCallback<T> callback)
			throws RichSQLException {
		return JDBCUtils.executeQuery(connection, query, callback);
	}

	/**
	 * Execute query.
	 * 
	 * @param sql
	 * @param params
	 * @param callback
	 * @return
	 * @throws RichSQLException
	 */
	public <T> T executeQuery(final String sql, final List<Object> params,
			final ResultSetCallback<T> callback)
			throws RichSQLException {
		return JDBCUtils.executeQuery(connection, sql, params, callback);
	}

	/**
	 * Execute query.
	 * 
	 * @param sql
	 * @param callback
	 * @return
	 * @throws RichSQLException
	 */
	public <T> T executeQuery(final String sql,
			final ResultSetCallback<T> callback)
			throws RichSQLException {
		return JDBCUtils.executeQuery(connection, sql, Collections.emptyList(),
				callback);
	}

}

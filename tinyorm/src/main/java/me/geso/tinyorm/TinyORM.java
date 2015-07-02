/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.geso.tinyorm;

import java.beans.IntrospectionException;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
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
import me.geso.jdbcutils.UncheckedRichSQLException;

/**
 * Tiny O/R Mapper implementation.
 * 
 * @author Tokuhiro Matsuno
 */
@Slf4j
public class TinyORM implements Closeable {

	private static final ConcurrentHashMap<Class<?>, TableMeta<?>> TABLE_META_REGISTRY = new ConcurrentHashMap<>();
	private final Connection connection;
	private Integer queryTimeout;

	public TinyORM(Connection connection) {
		this.connection = connection;
	}

	public Connection getConnection() {
		return this.connection;
	}

	public PreparedStatement prepareStatement(String sql) {
		try {
			final PreparedStatement preparedStatement = getConnection().prepareStatement(sql);
			if (queryTimeout != null) {
				preparedStatement.setQueryTimeout(queryTimeout);
			}
			return preparedStatement;
		} catch (SQLException e) {
			throw new UncheckedRichSQLException(e, sql, Collections.emptyList());
		}
	}

	/**
	 * Create {@code InsertStatement} for sending INSERT statement.<br>
	 * {@code orm.insert(Member.class)
	 * 	.value("name", "John")
	 * 	.execute();
	 * }
	 * 
	 * @param klass Row class to retrieve
	 * @return insert statement object
	 */
	public <T extends Row<?>> InsertStatement<T> insert(Class<T> klass) {
		TableMeta<T> tableMeta = this.getTableMeta(klass);
		return new InsertStatement<>(this, klass, tableMeta);
	}

	/**
	 * Select one row from the database.
	 */
	public <T extends Row<?>> Optional<T> singleBySQL(Class<T> klass,
			String sql,
			List<Object> params) {
		TableMeta<T> tableMeta = this.getTableMeta(klass);

		try (final PreparedStatement ps = this.prepareStatement(sql)) {
			JDBCUtils.fillPreparedStatementParams(ps, params);
			try (final ResultSet rs = ps.executeQuery()) {
				List<String> columnLabels = getColumnLabels(rs);
				if (rs.next()) {
					final T row = tableMeta.createRowFromResultSet(
							klass, rs,
							columnLabels, this);
					return Optional.of(row);
				} else {
					return Optional.<T>empty();
				}
			}
		} catch (final SQLException ex) {
			throw new UncheckedRichSQLException(ex, sql, params);
		}
	}

	/**
	 * Select one row from the database.
	 * 
	 * @param klass Row class to retrieve.
	 * @param query Query object
	 * @return Got value.
	 */
	public <T extends Row<?>> Optional<T> singleBySQL(Class<T> klass,
			Query query) {
		return this.singleBySQL(klass, query.getSQL(), query.getParameters());
	}

	/**
	 * Create new <code>BeanSelectStatement</code> for selecting 1 row.
	 * 
	 * @param klass
	 *            Target entity class.
	 * @return select statement object.
	 */
	public <T extends Row<?>> BeanSelectStatement<T> single(Class<T> klass) {
		TableMeta<T> tableMeta = this.getTableMeta(klass);
		return new BeanSelectStatement<>(this.getConnection(),
			klass, tableMeta, this);
	}

	/**
	 * Create new <code>ListSelectStatement</code> for selecting rows.
	 * 
	 * @param klass
	 *            Target entity class.
	 * @return Select statement object.
	 */
	public <T extends Row<?>> ListSelectStatement<T> search(Class<T> klass) {
		TableMeta<T> tableMeta = this.getTableMeta(klass);
		return new ListSelectStatement<>(this.getConnection(),
			klass, tableMeta, this);
	}

	/**
	 * Create new <code>PaginatedSelectStatement</code> for selecting rows.
	 * 
	 * @param klass Row class
	 * @return paginated select statement object
	 */
	public <T extends Row<?>> PaginatedSelectStatement<T> searchWithPager(
			final Class<T> klass, final long limit) {
		TableMeta<T> tableMeta = this.getTableMeta(klass);
		return new PaginatedSelectStatement<>(this.getConnection(),
			klass, tableMeta, this, limit);
	}

	/**
	 * Search by SQL.
	 */
	public <T extends Row<?>> List<T> searchBySQL(
			final Class<T> klass, final String sql, final List<Object> params) {
		try (final PreparedStatement ps = this.prepareStatement(sql)) {
			JDBCUtils.fillPreparedStatementParams(ps, params);
			try (final ResultSet rs = ps.executeQuery()) {
				return this.mapRowListFromResultSet(klass, rs);
			}
		} catch (final SQLException ex) {
			throw new UncheckedRichSQLException(ex, sql, params);
		}
	}

	/**
	 * Search by SQL.
	 * 
	 */
	public <T extends Row<?>> List<T> searchBySQL(final Class<T> klass,
			final String sql) {
		return this.searchBySQL(klass, sql, Collections.emptyList());
	}

	/**
	 * Search by SQL with Pager.
	 * 
	 * 
	 */
	public <T extends Row<?>> Paginated<T> searchBySQLWithPager(
			@NonNull final Class<T> klass, final String sql, final List<Object> params,
			final long entriesPerPage) {
		String limitedSql = sql + " LIMIT " + (entriesPerPage + 1);
		try (final PreparedStatement ps = this.prepareStatement(limitedSql)) {
			JDBCUtils.fillPreparedStatementParams(ps, params);
			try (final ResultSet rs = ps.executeQuery()) {
				List<T> rows = this.mapRowListFromResultSet(klass, rs);
				return new Paginated<>(rows, entriesPerPage);
			}
		} catch (final SQLException ex) {
			throw new UncheckedRichSQLException(ex, limitedSql, params);
		}
	}

	<T extends Row<?>> UpdateRowStatement<T> createUpdateStatement(T row) {
		return new UpdateRowStatement<>(row, this);
	}

	/**
	 * Execute an UPDATE, INSERT, and DELETE query.
	 * 
	 */
	public int updateBySQL(final String sql, final List<Object> params) {
		try (final PreparedStatement ps = this.prepareStatement(sql)) {
			JDBCUtils.fillPreparedStatementParams(ps, params);
			return ps.executeUpdate();
		} catch (final SQLException ex) {
			throw new UncheckedRichSQLException(ex, sql, params);
		}
	}

	/**
	 * Execute an UPDATE, INSERT, and DELETE query.
	 * 
	 */
	public int updateBySQL(String sql) {
		final List<Object> params = Collections.emptyList();
		try (final PreparedStatement ps = this.prepareStatement(sql)) {
			JDBCUtils.fillPreparedStatementParams(ps, params);
			return ps.executeUpdate();
		} catch (final SQLException ex) {
			throw new UncheckedRichSQLException(ex, sql, params);
		}
	}

	/**
	 * Execute an UPDATE, INSERT, and DELETE query.
	 * 
	 */
	public int updateBySQL(Query query) {
		final String sql = query.getSQL();
		final List<Object> params = query.getParameters();
		try (final PreparedStatement ps = this.prepareStatement(sql)) {
			JDBCUtils.fillPreparedStatementParams(ps, params);
			return ps.executeUpdate();
		} catch (final SQLException ex) {
			throw new UncheckedRichSQLException(ex, sql, params);
		}
	}

	<T extends Row<?>> List<T> mapRowListFromResultSet(Class<T> klass,
			ResultSet rs) throws SQLException {
		TableMeta<T> tableMeta = this.getTableMeta(klass);
		ArrayList<T> rows = new ArrayList<>();
		List<String> columnLabels = getColumnLabels(rs);
		while (rs.next()) {
			T row = tableMeta.createRowFromResultSet(klass, rs, columnLabels, this);
			rows.add(row);
		}
		return rows;
	}

	/**
	 * Select single long value
	 * 
	 * @return Got long value.
	 */
	public OptionalLong queryForLong(final String sql,
			@NonNull final List<Object> params) {
		try (final PreparedStatement ps = this.prepareStatement(sql)) {
			JDBCUtils.fillPreparedStatementParams(ps, params);
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final long l = rs.getLong(1);
					return OptionalLong.of(l);
				} else {
					return OptionalLong.empty();
				}
			}
		} catch (final SQLException ex) {
			throw new UncheckedRichSQLException(ex, sql, params);
		}
	}

	/**
	 * Select single long value from database.
	 * 
	 * @return Got value.
	 */
	public OptionalLong queryForLong(@NonNull final String sql) {
		return this.queryForLong(sql, Collections.emptyList());
	}

	/**
	 * Select single String value from database.
	 * 
	 * @return Got value
	 */
	public Optional<String> queryForString(final String sql,
			@NonNull final List<Object> params) {
		try (final PreparedStatement ps = this.prepareStatement(sql)) {
			JDBCUtils.fillPreparedStatementParams(ps, params);
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final String s = rs.getString(1);
					return Optional.of(s);
				} else {
					return Optional.<String>empty();
				}
			}
		} catch (final SQLException ex) {
			throw new UncheckedRichSQLException(ex, sql, params);
		}
	}

	/**
	 * Select single String value from database without parameters.
	 * 
	 * @return Got value
	 */
	public Optional<String> queryForString(@NonNull final String sql) {
		return this.queryForString(sql, Collections.emptyList());
	}

	public <T extends Row<?>> void delete(final T row) {
		@SuppressWarnings("unchecked")
		final TableMeta<T> tableMeta = this.getTableMeta((Class<T>)row.getClass());
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

		final String sql = query.getSQL();
		final List<Object> params = query.getParameters();
		final int result;
		try (final PreparedStatement ps = this.prepareStatement(sql)) {
			JDBCUtils.fillPreparedStatementParams(ps, params);
			result = ps.executeUpdate();
		} catch (final SQLException ex) {
			throw new UncheckedRichSQLException(ex, sql, params);
		}
		final int updated = result;
		if (updated != 1) {
			throw new RuntimeException("Cannot delete row: " + query);
		}
	}

	@SuppressWarnings("unchecked")
	<T extends Row<?>> Optional<T> refetch(final T row) {
		final TableMeta<T> tableMeta = this.getTableMeta((Class<T>)row.getClass());
		final String identifierQuoteString = this.getIdentifierQuoteString();
		final Query where = tableMeta.createWhereClauseFromRow(row,
				identifierQuoteString);

		final Query query = new QueryBuilder(identifierQuoteString)
			.appendQuery("SELECT * FROM ")
			.appendIdentifier(tableMeta.getName())
			.appendQuery(" WHERE ")
			.append(where)
			.build();

		final String sql = query.getSQL();
		final List<Object> params = query.getParameters();
		try (final PreparedStatement ps = this.prepareStatement(sql)) {
			JDBCUtils.fillPreparedStatementParams(ps, params);
			try (final ResultSet rs = ps.executeQuery()) {
				List<String> columnLabels = getColumnLabels(rs);
				if (rs.next()) {
					final T refetched = tableMeta
						.createRowFromResultSet(
								(Class<T>)row.getClass(), rs,
								columnLabels, this);
					return Optional.of(refetched);
				} else {
					return Optional.<T>empty();
				}
			}
		} catch (final SQLException ex) {
			throw new UncheckedRichSQLException(ex, sql, params);
		}
	}

	@SuppressWarnings("unchecked")
	<T extends Row<?>> TableMeta<T> getTableMeta(final Class<T> klass) {
		return (TableMeta<T>)TABLE_META_REGISTRY.computeIfAbsent(klass, key -> {
			log.info("Loading {}", klass);
			try {
				return TableMeta.build(klass);
			} catch (IntrospectionException e) {
				throw new RuntimeException(e);
			}
		});
	}

	/**
	 * Get table name from row meta
	 *
	 * @param rowClass row class
	 * @return table name
	 */
	public <T extends Row<?>> String getTableName(final Class<T> rowClass) {
		final TableMeta<T> tableMeta = this.getTableMeta(rowClass);
		return tableMeta.getName();
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
	 * Get column labels from result set.
	 * @param rs result set
	 * @return column label list
	 * @throws SQLException
	 */
	List<String> getColumnLabels(ResultSet rs) throws SQLException {
		List<String> columnLabels = new ArrayList<>();
		for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
			columnLabels.add(rs.getMetaData().getColumnLabel(i + 1));
		}
		return columnLabels;
	}

	/**
	 * Execute query builder.
	 *
	 * @return New query builder object
	 */
	public QueryBuilder createQueryBuilder() {
		return new QueryBuilder(this.getIdentifierQuoteString());
	}

	/**
	 * Execute query.
	 * 
	 * @param query Query object
	 * @param callback callback function to map ResultSet to Object.
	 * @return Fetched value.
	 */
	public <T> T executeQuery(final Query query,
			final ResultSetCallback<T> callback) {
		final String sql = query.getSQL();
		final List<Object> params = query.getParameters();
		try (final PreparedStatement ps = this.prepareStatement(sql)) {
			JDBCUtils.fillPreparedStatementParams(ps, params);
			try (final ResultSet rs = ps.executeQuery()) {
				return callback.call(rs);
			}
		} catch (final SQLException ex) {
			throw new UncheckedRichSQLException(ex, sql, params);
		}
	}

	/**
	 * Execute query.
	 * 
	 * @param sql SQL query
	 * @param params SQL parameters
	 * @param callback Callback function
	 * @return Selected value
	 */
	public <T> T executeQuery(final String sql, final List<Object> params,
			final ResultSetCallback<T> callback) {
		try (final PreparedStatement ps = this.prepareStatement(sql)) {
			JDBCUtils.fillPreparedStatementParams(ps, params);
			try (final ResultSet rs = ps.executeQuery()) {
				return callback.call(rs);
			}
		} catch (final SQLException ex) {
			throw new UncheckedRichSQLException(ex, sql, params);
		}
	}

	/**
	 * Execute query without callback.
	 * 
	 * @param sql SQL
	 */
	public void executeQuery(final String sql) {
		final List<Object> params = Collections.emptyList();
		try (final PreparedStatement ps = this.prepareStatement(sql)) {
			JDBCUtils.fillPreparedStatementParams(ps, params);
			final ResultSet resultSet = ps.executeQuery();
			resultSet.close();
		} catch (final SQLException ex) {
			throw new UncheckedRichSQLException(ex, sql, params);
		}
	}

	/**
	 * Execute query without callback.
	 * 
	 * @param sql SQL
	 * @param params Parameters
	 */
	public void executeQuery(final String sql, final List<Object> params) {
		try (final PreparedStatement ps = this.prepareStatement(sql)) {
			JDBCUtils.fillPreparedStatementParams(ps, params);
			final ResultSet rs = ps.executeQuery();
			rs.close();
		} catch (final SQLException ex) {
			throw new UncheckedRichSQLException(ex, sql, params);
		}
	}

	/**
	 * Execute query.
	 * 
	 * @param sql SQL
	 * @param callback Callback function
	 * @return Selected data
	 */
	public <T> T executeQuery(final String sql,
			final ResultSetCallback<T> callback) {
		final List<Object> params = Collections.emptyList();
		try (final PreparedStatement ps = this.prepareStatement(sql)) {
			JDBCUtils.fillPreparedStatementParams(ps, params);
			try (final ResultSet rs = ps.executeQuery()) {
				return callback.call(rs);
			}
		} catch (final SQLException ex) {
			throw new UncheckedRichSQLException(ex, sql, params);
		}
	}

	/**
	 * Count rows.
	 *
	 * <pre><code>
	 * 	long count = db.count(MemberRow.class)
	 * 		.where("status=?", 1)
	 * 		.execute();
	 * </code></pre>
	 * 
	 * @param klass row class.
	 * @return Instance of {@link me.geso.tinyorm.SelectCountStatement}.
	 */
	public <T extends Row<?>> SelectCountStatement<T> count(final Class<T> klass) {
		TableMeta<T> tableMeta = this.getTableMeta(klass);
		return new SelectCountStatement<>(tableMeta, this);
	}

	/**
	 * Close connection.
	 *
	 * @throws java.lang.RuntimeException This method throws RuntimeException if Connection#close throws SQLException.
	 */
	@Override
	public void close() throws IOException {
		try {
			this.connection.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public Integer getQueryTimeout() {
		return queryTimeout;
	}

	public void setQueryTimeout(int queryTimeout) {
		this.queryTimeout = queryTimeout;
	}

	public void clearQueryTimeout() {
		this.queryTimeout = null;
	}
}

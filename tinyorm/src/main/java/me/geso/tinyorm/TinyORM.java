package me.geso.tinyorm;

import java.beans.IntrospectionException;
import java.io.Closeable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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
import net.moznion.db.transaction.manager.TransactionManager;
import net.moznion.db.transaction.manager.TransactionScope;

import javax.inject.Provider;

/**
 * Tiny O/R Mapper implementation.
 *
 * @author Tokuhiro Matsuno
 */
@Slf4j
public class TinyORM implements Closeable {

	private static final ConcurrentHashMap<Class<?>, TableMeta<?>> TABLE_META_REGISTRY = new ConcurrentHashMap<>();
	private volatile Connection connection;
	private volatile Connection readConnection;
	private volatile TransactionManager transactionManager;
	private Integer queryTimeout;

	private Provider<Connection> connectionProvider;
	private Provider<Connection> readConnectionProvider;

	public TinyORM(Connection connection) {
		this.connection = connection;
		this.readConnection = connection;
		this.transactionManager = new TransactionManager(this.connection);
	}

	public TinyORM(Connection writeConnection, Connection readConnection) {
		this.connection = writeConnection;
		this.readConnection = readConnection;
		this.transactionManager = new TransactionManager(this.connection);
	}

	public TinyORM(Provider<Connection> connectionProvider) {
		this.connectionProvider = connectionProvider;
		// Do not assign to readConnectionProvider
	}

	public TinyORM(Provider<Connection> connectionProvider, Provider<Connection> readConnectionProvider) {
		this.connectionProvider = connectionProvider;
		this.readConnectionProvider = readConnectionProvider;
	}

	public Connection getConnection() {
		if (connection == null) {
			if (connectionProvider == null) {
				throw new RuntimeException("Connection provider is null");
			}

			synchronized (this) { // For multi-threads, protect from duplicated borrowing
				if (connection == null) {
					connection = connectionProvider.get();
					transactionManager = new TransactionManager(connection);
				}
				// otherwise, connection has been borrowed by another thread
			}
		}
		return connection;
	}

	public Connection getReadConnection() {
		try {
			if (connection != null && !connection.getAutoCommit()) {
				// If transaction has been started, return the connection which has taken the transaction.
				return connection;
			}
		} catch (SQLException e) {
			try {
				if (!connection.isClosed()) {
					// Not closed connection, DB access error is occured
					throw new RuntimeException("Failed to get the mode of auto commit", e);
				}
			} catch (SQLException e1) {
				throw new RuntimeException("Failed to get the mode of auto commit", e1);
			}
		}

		if (readConnection == null) {
			if (readConnectionProvider == null) {
				if (connectionProvider != null) {
					// For lazily borrowing with single connection
					readConnection = getConnection(); // use the same connection as write/read
					return readConnection;
				}

				throw new RuntimeException("Read connection provider is null");
			}

			synchronized (this) { // For multi-threads, protect from duplicated borrowing
				if (readConnection == null) {
					readConnection = readConnectionProvider.get();
				}
				// otherwise, connection has been borrowed by another thread
			}
		}
		return readConnection;
	}

	public TransactionManager getTransactionManager() {
		if (transactionManager == null) {
			getConnection();
		}
		return transactionManager;
	}

	public PreparedStatement prepareStatement(String sql) {
		return prepareStatement(sql, getConnection());
	}

	public PreparedStatement prepareStatementForRead(String sql) {
		return prepareStatement(sql, getReadConnection());
	}

	PreparedStatement prepareStatement(String sql, Connection connection) {
		try {
			final PreparedStatement preparedStatement = connection.prepareStatement(sql);
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
	public <T extends Row<?>> Optional<T> singleBySQL(Class<T> klass, String sql, List<Object> params) {
		return singleBySQL(klass, sql, params, getReadConnection());
	}

	public <T extends Row<?>> Optional<T> singleBySQL(
			Class<T> klass, String sql, List<Object> params, Connection connection) {
		TableMeta<T> tableMeta = this.getTableMeta(klass);

		try (final PreparedStatement ps = this.prepareStatement(sql, connection)) {
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
	public <T extends Row<?>> Optional<T> singleBySQL(Class<T> klass, Query query) {
		return this.singleBySQL(klass, query.getSQL(), query.getParameters());
	}

	/**
	 * Select one row from the database by specified connection.
	 *
	 * NOTE: for select query with `last_insert_id()`.
	 *
	 * @param klass Row class to retrieve.
	 * @param query Query object.
	 * @param connection Connection to retrieve row.
     * @return Got value.
     */
	public <T extends Row<?>> Optional<T> singleBySQL(Class<T> klass, Query query, Connection connection) {
		return this.singleBySQL(klass, query.getSQL(), query.getParameters(), connection);
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
		BeanSelectStatement<T> statement =  new BeanSelectStatement<>(
			getReadConnection(), klass, tableMeta, this);

		// ensure at most single result for single(). (as default behavior)
		statement.limit(1);

		return statement;
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
		return new ListSelectStatement<>(getReadConnection(),
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
		return new PaginatedSelectStatement<>(getReadConnection(),
			klass, tableMeta, this, limit);
	}

	/**
	 * Search by SQL.
	 */
	public <T extends Row<?>> List<T> searchBySQL(final Class<T> klass, final String sql, final List<Object> params) {
		return searchBySQL(klass, sql, params, getReadConnection());
	}

	/**
	 * Search by SQL.
	 */
	public <T extends Row<?>> List<T> searchBySQL(final Class<T> klass, final String sql, final List<Object> params,
			final Connection connection) {
		try (final PreparedStatement ps = prepareStatement(sql, connection)) {
			JDBCUtils.fillPreparedStatementParams(ps, params);
			try (final ResultSet rs = ps.executeQuery()) {
				return mapRowListFromResultSet(klass, rs);
			}
		} catch (final SQLException ex) {
			throw new UncheckedRichSQLException(ex, sql, params);
		}
	}

	/**
	 * Search by SQL.
	 */
	public <T extends Row<?>> List<T> searchBySQL(final Class<T> klass, final String sql) {
		return searchBySQL(klass, sql, Collections.emptyList());
	}

	/**
	 * Search by SQL.
	 */
	public <T extends Row<?>> List<T> searchBySQL(final Class<T> klass, final String sql, final Connection connection) {
		return searchBySQL(klass, sql, Collections.emptyList(), connection);
	}

	/**
	 * Search by SQL with Pager.
	 */
	public <T extends Row<?>> Paginated<T> searchBySQLWithPager(@NonNull final Class<T> klass, final String sql,
			final List<Object> params, final long entriesPerPage) {
		return searchBySQLWithPager(klass, sql, params, entriesPerPage, getReadConnection());
	}

	public <T extends Row<?>> Paginated<T> searchBySQLWithPager(@NonNull final Class<T> klass, final String sql,
			final List<Object> params, final long entriesPerPage, final Connection connection) {
		String limitedSql = sql + " LIMIT " + (entriesPerPage + 1);
		try (final PreparedStatement ps = this.prepareStatement(limitedSql, connection)) {
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

	<T extends Row<?>> List<T> mapRowListFromResultSet(Class<T> klass, ResultSet rs) throws SQLException {
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
	public OptionalLong queryForLong(final String sql, @NonNull final List<Object> params) {
		return queryForLong(sql, params, getReadConnection());
	}

	/**
	 * Select single long value
	 *
	 * @return Got long value.
	 */
	public OptionalLong queryForLong(final String sql, @NonNull final List<Object> params,
			final Connection connection) {
		try (final PreparedStatement ps = prepareStatement(sql, connection)) {
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
		return queryForLong(sql, Collections.emptyList());
	}

	/**
	 * Select single long value from database.
	 *
	 * @return Got value.
	 */
	public OptionalLong queryForLong(@NonNull final String sql, final Connection connection) {
		return queryForLong(sql, Collections.emptyList(), connection);
	}

	/**
	 * Select single String value from database.
	 *
	 * @return Got value
	 */
	public Optional<String> queryForString(final String sql, @NonNull final List<Object> params) {
		return queryForString(sql, params, getReadConnection());
	}

	/**
	 * Select single String value from database.
	 *
	 * @return Got value
	 */
	public Optional<String> queryForString(final String sql, @NonNull final List<Object> params,
			final Connection connection) {
		try (final PreparedStatement ps = prepareStatement(sql, connection)) {
			JDBCUtils.fillPreparedStatementParams(ps, params);
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final String s = rs.getString(1);
					return Optional.ofNullable(s);
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
		return queryForString(sql, Collections.emptyList());
	}

	/**
	 * Select single String value from database without parameters.
	 *
	 * @return Got value
	 */
	public Optional<String> queryForString(@NonNull final String sql, final Connection connection) {
		return queryForString(sql, Collections.emptyList(), connection);
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

	<T extends Row<?>> Optional<T> refetch(final T row) {
	    return refetch(row, getReadConnection());
	}

	@SuppressWarnings("unchecked")
	<T extends Row<?>> Optional<T> refetch(final T row, final Connection connection) {
		final TableMeta<T> tableMeta = this.getTableMeta((Class<T>)row.getClass());
		final String identifierQuoteString = this.getIdentifierQuoteString();
		final Query where = tableMeta.createWhereClauseFromRow(row, identifierQuoteString);

		final Query query = new QueryBuilder(identifierQuoteString)
				.appendQuery("SELECT * FROM ")
				.appendIdentifier(tableMeta.getName())
				.appendQuery(" WHERE ")
				.append(where)
				.build();

		final String sql = query.getSQL();
		final List<Object> params = query.getParameters();
		try (final PreparedStatement ps = prepareStatement(sql, connection)) {
			JDBCUtils.fillPreparedStatementParams(ps, params);
			try (final ResultSet rs = ps.executeQuery()) {
				List<String> columnLabels = getColumnLabels(rs);
				if (rs.next()) {
					final T refetched = tableMeta.createRowFromResultSet((Class<T>)row.getClass(), rs, columnLabels,
							this);
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
			return getConnection().getMetaData()
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
	static public List<String> getColumnLabels(ResultSet rs) throws SQLException {
		ResultSetMetaData metaData = rs.getMetaData();
		List<String> columnLabels = new ArrayList<>(metaData.getColumnCount());
		for (int i = 0; i < metaData.getColumnCount(); i++) {
			columnLabels.add(metaData.getColumnLabel(i + 1));
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
	public <T> T executeQuery(final Query query, final ResultSetCallback<T> callback) {
		return executeQuery(query.getSQL(), Collections.emptyList(), callback, getReadConnection());
	}

	/**
	 * Execute query.
	 *
	 * @param query Query object
	 * @param callback callback function to map ResultSet to Object.
	 * @param connection Database connection to use.
	 * @return Fetched value.
	 */
	public <T> T executeQuery(final Query query, final ResultSetCallback<T> callback, final Connection connection) {
		return executeQuery(query.getSQL(), Collections.emptyList(), callback, connection);
	}

	/**
	 * Execute query.
	 *
	 * @param sql SQL query
	 * @param params SQL parameters
	 * @param callback Callback function
	 * @return Selected value
	 */
	public <T> T executeQuery(final String sql, final List<Object> params, final ResultSetCallback<T> callback) {
		return executeQuery(sql, params, callback, getReadConnection());
	}

	/**
	 * Execute query.
	 *
	 * @param sql SQL query
	 * @param params SQL parameters
	 * @param callback Callback function
	 * @param connection Database connection to use.
	 * @return Selected value
	 */
	public <T> T executeQuery(final String sql, final List<Object> params, final ResultSetCallback<T> callback,
			final Connection connection) {
		try (final PreparedStatement ps = prepareStatement(sql, connection)) {
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
	 * @param sql SQL
	 * @param callback Callback function
	 * @return Selected data
	 */
	public <T> T executeQuery(final String sql, final ResultSetCallback<T> callback) {
		final List<Object> params = Collections.emptyList();
		return executeQuery(sql, params, callback, getReadConnection());
	}

	/**
	 * Execute query.
	 *
	 * @param sql SQL
	 * @param callback Callback function
	 * @return Selected data
	 */
	public <T> T executeQuery(final String sql, final ResultSetCallback<T> callback, final Connection connection) {
		return executeQuery(sql, Collections.emptyList(), callback, connection);
	}

	/**
	 * Execute query without callback.
	 *
	 * @param sql SQL
	 */
	public void executeQuery(final String sql) {
		executeQuery(sql, Collections.emptyList(), getReadConnection());
	}

	/**
	 * Execute query without callback.
	 *
	 * @param sql SQL
	 * @param connection Database connection to use.
	 */
	public void executeQuery(final String sql, final Connection connection) {
		executeQuery(sql, Collections.emptyList(), connection);
	}

	/**
	 * Execute query without callback.
	 *
	 * @param sql SQL
	 * @param params Parameters
	 */
	public void executeQuery(final String sql, final List<Object> params) {
		executeQuery(sql, params, getReadConnection());
	}

	/**
	 * Execute query without callback.
	 *
	 * @param sql SQL
	 * @param params Parameters
	 */
	public void executeQuery(final String sql, final List<Object> params, final Connection connection) {
		try (final PreparedStatement ps = prepareStatement(sql, connection)) {
			JDBCUtils.fillPreparedStatementParams(ps, params);
			final ResultSet rs = ps.executeQuery();
			rs.close();
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
	public void close() {
		try {
			if (connection != null) {
				connection.close();
			}

			if (readConnection != null) {
				if (!readConnection.isClosed()) {
					readConnection.close();
				}
			}
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

	/**
	 * Begin transaction.
	 * <p>
	 * This method backups automatically the status of auto commit mode when called.
	 * The status will be turned back when transaction is end.
	 *
	 * @throws SQLException
	 */
	public void transactionBegin() throws SQLException {
		getTransactionManager().txnBegin();
	}

	/**
	 * Create a new transaction scope.
	 *
	 * <pre>
	 * {@code
	 * try (TransactionScope txn = db.createTransactionScope()) {
	 *     db.insert(Member.class)
	 * 	       .value("name", "John")
	 * 	       .execute();
	 *     db.transactionCommit();
	 * }
	 * }
	 * </pre>
	 *
	 * <p>
	 * If it escapes from try-with-resource statement without any action
	 * ({@code transactionCommit()} or {@code transactionRollback()}),
	 * transaction will rollback automatically.
	 *
	 * @throws SQLException
	 * @return a transaction scope
	 */
	public TransactionScope createTransactionScope() throws SQLException {
		return new TransactionScope(getTransactionManager());
	}

	/**
	 * Commit a current transaction.
	 *
	 * @throws SQLException
	 */
	public void transactionCommit() throws SQLException {
		getTransactionManager().txnCommit();
	}

	/**
	 * Rollback a current transaction.
	 *
	 * @throws SQLException
	 */
	public void transactionRollback() throws SQLException {
		getTransactionManager().txnRollback();
	}
}

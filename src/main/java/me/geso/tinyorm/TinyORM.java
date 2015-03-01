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

	private static final ConcurrentHashMap<Class<?>, TableMeta<?>> TABLE_META_REGISTRY = new ConcurrentHashMap<>();
	private final Connection connection;

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

		try {
			return JDBCUtils.executeQuery(
				this.connection,
				sql,
				params,
				(rs) -> {
					if (rs.next()) {
						final T row = tableMeta.createRowFromResultSet(
							klass,
							rs, this);
						return Optional.of(row);
					} else {
						return Optional.<T>empty();
					}
				});
		} catch (RichSQLException e) {
			throw new RuntimeException(e);
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
		try {
			return JDBCUtils.executeQuery(this.connection, sql, params,
				(rs) -> this.mapRowListFromResultSet(klass, rs));
		} catch (RichSQLException e) {
			throw new RuntimeException(e);
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
		Connection connection = this.getConnection();
		try {
			return JDBCUtils.executeQuery(connection, limitedSql, params,
				(rs) -> {
					List<T> rows = this.mapRowListFromResultSet(klass, rs);
					return new Paginated<>(rows, entriesPerPage);
				});
		} catch (RichSQLException e) {
			throw new RuntimeException(e);
		}
	}

	<T extends Row<?>> UpdateRowStatement<T> createUpdateStatement(T row) {
		@SuppressWarnings("unchecked")
		TableMeta<T> tableMeta = this.getTableMeta((Class<T>)row.getClass());
		return new UpdateRowStatement<>(row,
			this.getConnection(), tableMeta,
			this.getIdentifierQuoteString());
	}

	/**
	 * Execute an UPDATE, INSERT, and DELETE query.
	 * 
	 */
	public int updateBySQL(final String sql, final List<Object> params) {
		try {
			return JDBCUtils.executeUpdate(this.connection, sql, params);
		} catch (RichSQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Execute an UPDATE, INSERT, and DELETE query.
	 * 
	 */
	public int updateBySQL(String sql) {
		try {
			return JDBCUtils.executeUpdate(this.connection, sql);
		} catch (RichSQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Execute an UPDATE, INSERT, and DELETE query.
	 * 
	 */
	public int updateBySQL(Query query) {
		try {
			return JDBCUtils.executeUpdate(this.connection, query);
		} catch (RichSQLException e) {
			throw new RuntimeException(e);
		}
	}

	<T extends Row<?>> List<T> mapRowListFromResultSet(Class<T> klass,
			ResultSet rs) throws SQLException {
		TableMeta<T> tableMeta = this.getTableMeta(klass);
		ArrayList<T> rows = new ArrayList<>();
		while (rs.next()) {
			T row = tableMeta.createRowFromResultSet(klass, rs, this);
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
		try {
			return JDBCUtils.executeQuery(this.connection, sql, params,
				(rs) -> {
					if (rs.next()) {
						final long l = rs.getLong(1);
						return OptionalLong.of(l);
					} else {
						return OptionalLong.empty();
					}
				});
		} catch (RichSQLException e) {
			throw new RuntimeException(e);
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
		try {
			return JDBCUtils.executeQuery(this.connection, sql, params,
				(rs) -> {
					if (rs.next()) {
						final String s = rs.getString(1);
						return Optional.of(s);
					} else {
						return Optional.<String>empty();
					}
				});
		} catch (RichSQLException e) {
			throw new RuntimeException(e);
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
		final Connection connection = this.getConnection();
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

		try {
			final int updated = JDBCUtils.executeUpdate(connection, query);
			if (updated != 1) {
				throw new RuntimeException("Cannot delete row: " + query);
			}
		} catch (RichSQLException e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	<T extends Row<?>> Optional<T> refetch(final T row) {
		final Connection connection = this.getConnection();
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

		try {
			return JDBCUtils
				.executeQuery(
					connection,
					query,
					(rs) -> {
						if (rs.next()) {
							final T refetched = tableMeta
								.createRowFromResultSet(
									(Class<T>)row.getClass(),
									rs, this);
							return Optional.of(refetched);
						} else {
							return Optional.<T>empty();
						}
					});
		} catch (RichSQLException e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	<T extends Row<?>> TableMeta<T> getTableMeta(final Class<T> klass) {
		return (TableMeta<T>)TABLE_META_REGISTRY.computeIfAbsent(klass, key -> {
			log.info("Loading {}", klass);
			try {
				return TableMeta.build(klass);
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
		try {
			return JDBCUtils.executeQuery(this.connection, query, callback);
		} catch (RichSQLException e) {
			throw new RuntimeException(e);
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
		try {
			return JDBCUtils.executeQuery(this.connection, sql, params,
				callback);
		} catch (RichSQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Execute query without callback.
	 * 
	 * @param sql SQL
	 */
	public void executeQuery(final String sql) {
		try {
			JDBCUtils.executeQuery(this.connection, sql,
				Collections.emptyList());
		} catch (RichSQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Execute query without callback.
	 * 
	 * @param sql SQL
	 * @param params Parameters
	 */
	public void executeQuery(final String sql, final List<Object> params) {
		try {
			JDBCUtils.executeQuery(this.connection, sql, params);
		} catch (RichSQLException e) {
			throw new RuntimeException(e);
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
		try {
			return JDBCUtils.executeQuery(this.connection, sql,
				Collections.emptyList(),
				callback);
		} catch (RichSQLException e) {
			throw new RuntimeException(e);
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
		return new SelectCountStatement<>(tableMeta, this.getConnection());
	}

}

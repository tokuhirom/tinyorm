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
import me.geso.jdbcutils.RichSQLException;

/**
 * Tiny O/R Mapper implementation.
 * 
 * @author Tokuhiro Matsuno
 */
@Slf4j
public class TinyORM {

	private final Connection connection;

	private static ConcurrentHashMap<Class<?>, TableMeta> tableMetaRegistry = new ConcurrentHashMap<>();

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
	public <T> InsertStatement<T> insert(Class<T> klass) {
		return new InsertStatement<>(this, klass, this.getTableMeta(klass));
	}

	/**
	 * Select one row from the database.
	 * 
	 * @throws SQLException
	 * @throws RichSQLException
	 */
	public <T> Optional<T> singleBySQL(Class<T> klass, String sql,
			List<Object> params) throws RichSQLException {
		return JDBCUtils.executeQuery(connection, sql, params, (rs) -> {
			TableMeta tableMeta = this.getTableMeta(klass);
			if (rs.next()) {
				T row = this.mapRowFromResultSet(klass, rs, tableMeta);
				return Optional.of(row);
			} else {
				return Optional.empty();
			}
		});
	}

	/**
	 * Create new <code>BeanSelectStatement</code> for selecting 1 row.
	 * 
	 * @param klass
	 *            Target entity class.
	 * @return
	 */
	public <T> BeanSelectStatement<T> single(Class<T> klass) {
		TableMeta tableMeta = this.getTableMeta(klass);
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
	public <T> ListSelectStatement<T> search(Class<T> klass) {
		TableMeta tableMeta = this.getTableMeta(klass);
		return new ListSelectStatement<>(this.getConnection(),
				klass, tableMeta, this);
	}

	/**
	 * Create new <code>PaginatedSelectStatement</code> for selecting rows.
	 * 
	 * @param klass
	 * @return
	 */
	public <T> PaginatedSelectStatement<T> searchWithPager(
			final Class<T> klass, final long limit) {
		TableMeta tableMeta = this.getTableMeta(klass);
		return new PaginatedSelectStatement<>(this.getConnection(),
				klass, tableMeta, this, limit);
	}

	/**
	 * Search by SQL.
	 * 
	 * @throws RichSQLException
	 * 
	 */
	public <T> List<T> searchBySQL(
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
	public <T> List<T> searchBySQL(final Class<T> klass, final String sql)
			throws RichSQLException {
		return this.searchBySQL(klass, sql, Collections.emptyList());
	}

	/**
	 * Search by SQL with Pager.
	 * 
	 * @throws RichSQLException
	 * 
	 */
	public <T> Paginated<T> searchBySQLWithPager(
			final Class<T> klass, final String sql, final List<Object> params,
			final long entriesPerPage) throws RichSQLException {
		String limitedSql = sql + " LIMIT " + (entriesPerPage + 1);
		Connection connection = this.getConnection();
		return JDBCUtils.executeQuery(connection, limitedSql, params, (rs) -> {
			List<T> rows = this.mapRowListFromResultSet(klass, rs);
			return new Paginated<T>(rows, entriesPerPage);
		});
	}

	UpdateRowStatement createUpdateStatement(Object row) {
		TableMeta tableMeta = this.getTableMeta(row.getClass());
		UpdateRowStatement stmt = new UpdateRowStatement(row,
				this.getConnection(), tableMeta);
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

	<T> List<T> mapRowListFromResultSet(Class<T> klass, ResultSet rs)
			throws SQLException {
		TableMeta tableMeta = this.getTableMeta(klass);
		ArrayList<T> rows = new ArrayList<>();
		while (rs.next()) {
			T row = this.mapRowFromResultSet(klass, rs, tableMeta);
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
	<T> T mapRowFromResultSet(Class<T> klass, ResultSet rs) throws SQLException {
		TableMeta tableMeta = this.getTableMeta(klass);
		return this.mapRowFromResultSet(klass, rs, tableMeta);
	}

	<T> T mapRowFromResultSet(final Class<T> klass, final ResultSet rs,
			final TableMeta tableMeta) throws SQLException {
		try {
			int columnCount = rs.getMetaData().getColumnCount();
			T row = klass.newInstance();
			for (int i = 0; i < columnCount; ++i) {
				String columnName = rs.getMetaData().getColumnName(i + 1);
				Object value = rs.getObject(i + 1);
				value = tableMeta.invokeInflater(columnName, value);
				tableMeta.setValue(row, columnName, value);
			}
			if (row instanceof ORMInjectable) {
				((ORMInjectable) row).setOrm(this);
			}
			return row;
		} catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Select single long value
	 * 
	 * @return
	 * @throws SQLException
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
	 * @param string
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
	 * @throws SQLException
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
	 * @throws SQLException
	 * @throws RichSQLException
	 */
	public Optional<String> queryForString(@NonNull final String sql)
			throws RichSQLException {
		return this.queryForString(sql, Collections.emptyList());
	}

	public void delete(final Object row) throws RichSQLException {
		final Connection connection = this.getConnection();
		final TableMeta tableMeta = this.getTableMeta(row.getClass());
		final String tableName = tableMeta.getName();
		final Query where = tableMeta.createWhereClauseFromRow(row, connection);

		final StringBuilder buf = new StringBuilder();
		buf.append("DELETE FROM ")
				.append(TinyORMUtils.quoteIdentifier(tableName, connection))
				.append(" WHERE ");
		buf.append(where.getSQL());
		final String sql = buf.toString();

		final int updated = JDBCUtils.executeUpdate(connection, sql,
				where.getParameters());
		if (updated != 1) {
			throw new RuntimeException("Cannot delete row: " + sql
					+ " "
					+ where.getParameters());
		}
	}

	@SuppressWarnings("unchecked")
	<T> Optional<T> refetch(final T row) throws RichSQLException {
		final Connection connection = this.getConnection();
		final TableMeta tableMeta = this.getTableMeta(row.getClass());
		final Query where = tableMeta.createWhereClauseFromRow(row, connection);

		final StringBuilder buf = new StringBuilder();
		buf.append("SELECT * FROM ").append(
				TinyORMUtils.quoteIdentifier(tableMeta.getName(), connection));
		buf.append(" WHERE ").append(where.getSQL());
		final String sql = buf.toString();

		final List<Object> params = where.getParameters();
		return JDBCUtils.executeQuery(connection, sql, params, (rs) -> {
			if (rs.next()) {
				final T refetched = this.mapRowFromResultSet(
						(Class<T>) row.getClass(),
						rs, tableMeta);
				return Optional.of(refetched);
			} else {
				return Optional.empty();
			}
		});
	}

	TableMeta getTableMeta(final Class<?> klass) {
		return tableMetaRegistry.computeIfAbsent(klass, key -> {
			log.info("Loading {}", klass);
			try {
				return TableMeta.build(klass);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
	}

}

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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.slf4j.Slf4j;

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
	 */
	public <T> Optional<T> singleBySQL(Class<T> klass, String sql,
			Object[] params) {
		try {
			Connection connection = this.getConnection();
			try (PreparedStatement preparedStatement = connection
					.prepareStatement(sql)) {
				TinyORMUtil.fillPreparedStatementParams(preparedStatement,
						params);
				try (ResultSet rs = preparedStatement.executeQuery()) {
					TableMeta tableMeta = this.getTableMeta(klass);
					if (rs.next()) {
						T row = this.mapRowFromResultSet(klass, rs, tableMeta);
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
	 */
	public <T> List<T> searchBySQL(
			final Class<T> klass, final String sql, final Object[] params) {
		Connection connection = this.getConnection();
		try (PreparedStatement ps = connection.prepareStatement(sql)) {
			TinyORMUtil.fillPreparedStatementParams(ps, params);
			try (ResultSet rs = ps.executeQuery()) {
				List<T> rows = this.mapRowListFromResultSet(klass, rs);
				return rows;
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Search by SQL with Pager.
	 * 
	 */
	public <T> Paginated<T> searchBySQLWithPager(
			final Class<T> klass, final String sql, final Object[] params,
			final long entriesPerPage) {
		String limitedSql = sql + " LIMIT " + (entriesPerPage + 1);
		Connection connection = this.getConnection();
		try (PreparedStatement ps = connection.prepareStatement(limitedSql)) {
			TinyORMUtil.fillPreparedStatementParams(ps, params);
			try (ResultSet rs = ps.executeQuery()) {
				List<T> rows = this.mapRowListFromResultSet(klass, rs);
				return new Paginated<T>(rows, entriesPerPage);
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	UpdateRowStatement createUpdateStatement(Object row) {
		TableMeta tableMeta = this.getTableMeta(row.getClass());
		UpdateRowStatement stmt = new UpdateRowStatement(row,
				this.getConnection(), tableMeta);
		return stmt;
	}

	/**
	 * Execute an UPDATE, INSERT, and DELETE query.
	 */
	public int updateBySQL(String sql, Object[] params) {
		try (PreparedStatement preparedStatement = this.getConnection()
				.prepareStatement(sql)) {
			TinyORMUtil.fillPreparedStatementParams(preparedStatement, params);
			return preparedStatement.executeUpdate();
		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}

	<T> List<T> mapRowListFromResultSet(Class<T> klass, ResultSet rs) {
		TableMeta tableMeta = this.getTableMeta(klass);
		try {
			ArrayList<T> rows = new ArrayList<>();
			while (rs.next()) {
				T row = this.mapRowFromResultSet(klass, rs, tableMeta);
				rows.add(row);
			}
			return rows;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Shortcut method.
	 * 
	 * @param klass
	 * @param rs
	 * @return
	 */
	<T> T mapRowFromResultSet(Class<T> klass, ResultSet rs) {
		TableMeta tableMeta = this.getTableMeta(klass);
		return this.mapRowFromResultSet(klass, rs, tableMeta);
	}

	<T> T mapRowFromResultSet(Class<T> klass, ResultSet rs,
			TableMeta tableMeta) {
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
		} catch (SQLException | InstantiationException | IllegalAccessException e) {
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

	public void delete(Object row) {
		try {
			Connection connection = this.getConnection();
			TableMeta tableMeta = this.getTableMeta(row.getClass());
			String tableName = tableMeta.getName();
			Query where = tableMeta.createWhereClauseFromRow(row, connection);

			StringBuilder buf = new StringBuilder();
			buf.append("DELETE FROM ")
					.append(TinyORMUtil.quoteIdentifier(tableName, connection))
					.append(" WHERE ");
			buf.append(where.getSQL());
			String sql = buf.toString();

			try (PreparedStatement preparedStatement = connection
					.prepareStatement(sql)) {
				TinyORMUtil.fillPreparedStatementParams(preparedStatement,
						where.getValues());
				int updated = preparedStatement
						.executeUpdate();
				if (updated != 1) {
					throw new RuntimeException("Cannot delete row: " + sql
							+ " "
							+ Arrays.toString(where.getValues()));
				}
			}
		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}

	<T> Optional<T> refetch(T row) {
		Connection connection = this.getConnection();
		TableMeta tableMeta = this.getTableMeta(row.getClass());
		Query where = tableMeta.createWhereClauseFromRow(row, connection);

		StringBuilder buf = new StringBuilder();
		buf.append("SELECT * FROM ").append(
				TinyORMUtil.quoteIdentifier(tableMeta.getName(), connection));
		buf.append(" WHERE ").append(where.getSQL());
		String sql = buf.toString();

		try {
			Object[] params = where.getValues();
			try (PreparedStatement preparedStatement = connection
					.prepareStatement(sql)) {
				TinyORMUtil.fillPreparedStatementParams(preparedStatement,
						params);
				try (ResultSet rs = preparedStatement
						.executeQuery()) {
					if (rs.next()) {
						@SuppressWarnings("unchecked")
						T refetched = this.mapRowFromResultSet(
								(Class<T>) row.getClass(),
								rs, tableMeta);
						return Optional.of(refetched);
					} else {
						return Optional.empty();
					}
				}
			}
		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}

	TableMeta getTableMeta(Class<?> klass) {
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

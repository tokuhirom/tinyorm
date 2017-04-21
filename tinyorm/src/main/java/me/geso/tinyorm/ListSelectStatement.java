package me.geso.tinyorm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Stream;

import me.geso.jdbcutils.JDBCUtils;
import me.geso.jdbcutils.Query;
import me.geso.jdbcutils.RichSQLException;
import me.geso.jdbcutils.UncheckedRichSQLException;

public class ListSelectStatement<T extends Row<?>> extends
		AbstractSelectStatement<T, ListSelectStatement<T>> {

	private final TableMeta<T> tableMeta;
	private final TinyORM orm;
	private final Class<T> klass;

	ListSelectStatement(Connection connection,
			Class<T> klass, TableMeta<T> tableMeta, TinyORM orm) {
		super(connection, tableMeta.getName());
		this.tableMeta = tableMeta;
		this.orm = orm;
		this.klass = klass;
	}

	public List<T> execute() {
		final Query query = this.buildQuery();

		final String sql = query.getSQL();
		final List<Object> params = query.getParameters();
		try (final PreparedStatement ps = isForUpdate() || isForceWriteConnection() ? orm.prepareStatement(sql) :
				orm.prepareStatementForRead(sql)) {
			JDBCUtils.fillPreparedStatementParams(ps, params);
			try (final ResultSet rs = ps.executeQuery()) {
                return orm.mapRowListFromResultSet(klass, rs);
            }
		} catch (final SQLException ex) {
			throw new UncheckedRichSQLException(ex, sql, params);
		}
	}

	/**
	 * Create stream from select statement.
	 * You must close the stream after use. I mean you should use try-with-resources for return value from this method.
	 *
	 * @return stream, that generates row objects.
	 */
	public Stream<T> executeStream() {
		final Query query = this.buildQuery();

		final String sql = query.getSQL();
		final List<Object> params = query.getParameters();
		try {
			final PreparedStatement ps = isForUpdate() || isForceWriteConnection() ? orm.prepareStatement(sql) :
				 orm.prepareStatementForRead(sql);
			JDBCUtils.fillPreparedStatementParams(ps, params);

			final ResultSet rs = ps.executeQuery();
			List<String> columnLabels = TinyORM.getColumnLabels(rs);
			ResultSetIterator<T> iterator = new ResultSetIterator<>(ps, rs, sql, params,
					resultSet -> tableMeta.createRowFromResultSet(klass, resultSet, columnLabels, orm)
			);
            return iterator.toStream();
		} catch (SQLException e) {
			throw new RuntimeException(new RichSQLException(e, sql, params));
		}
	}
}

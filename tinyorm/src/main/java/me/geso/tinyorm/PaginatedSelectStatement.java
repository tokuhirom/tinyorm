package me.geso.tinyorm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import me.geso.jdbcutils.JDBCUtils;
import me.geso.jdbcutils.Query;
import me.geso.jdbcutils.UncheckedRichSQLException;

public class PaginatedSelectStatement<T extends Row<?>> extends
		AbstractSelectStatement<T, PaginatedSelectStatement<T>> {

	private final TinyORM orm;
	private final long entriesPerPage;
	private final Class<T> klass;

	PaginatedSelectStatement(Connection connection,
			Class<T> klass, TableMeta<T> tableMeta, TinyORM orm,
			long entriesPerPage) {
		super(connection, tableMeta.getName());
		this.klass = klass;
		this.orm = orm;
		this.entriesPerPage = entriesPerPage;
	}

	public Paginated<T> execute() {
		final Query query = this.limit(entriesPerPage + 1).buildQuery();

		final String sql = query.getSQL();
		final List<Object> params = query.getParameters();
		try (final PreparedStatement ps = isForUpdate() ? orm.prepareStatement(sql) : orm.prepareStatementForRead(sql)) {
			JDBCUtils.fillPreparedStatementParams(ps, params);
			try (final ResultSet rs = ps.executeQuery()) {
				List<T> rows = orm.mapRowListFromResultSet(klass, rs);

				return new Paginated<>(
						rows, entriesPerPage);
			}
		} catch (final SQLException ex) {
			throw new UncheckedRichSQLException(ex, sql, params);
		}
	}
}

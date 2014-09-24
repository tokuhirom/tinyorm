package me.geso.tinyorm;

import java.sql.Connection;
import java.util.List;

import me.geso.jdbcutils.JDBCUtils;
import me.geso.jdbcutils.Query;
import me.geso.jdbcutils.RichSQLException;

public class PaginatedSelectStatement<T extends Row<?>> extends
		AbstractSelectStatement<T, PaginatedSelectStatement<T>> {

	private final TinyORM orm;
	private final long entriesPerPage;
	private final Class<T> klass;
	private final Connection connection;

	PaginatedSelectStatement(Connection connection,
			Class<T> klass, TableMeta<T> tableMeta, TinyORM orm,
			long entriesPerPage) {
		super(connection, tableMeta.getName());
		this.klass = klass;
		this.orm = orm;
		this.entriesPerPage = entriesPerPage;
		this.connection = connection;
	}

	public Paginated<T> execute() {
		try {
			final Query query = this.limit(entriesPerPage + 1).buildQuery();

			return JDBCUtils.executeQuery(connection, query, (rs) -> {
				List<T> rows = orm.mapRowListFromResultSet(klass, rs);

				final Paginated<T> paginated = new Paginated<T>(
						rows, entriesPerPage);
				return paginated;
			});
		} catch (RichSQLException e) {
			throw new RuntimeException(e);
		}
	}
}

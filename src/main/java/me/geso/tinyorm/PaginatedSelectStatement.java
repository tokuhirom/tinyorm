package me.geso.tinyorm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class PaginatedSelectStatement<T> extends
		AbstractSelectStatement<T, PaginatedSelectStatement<T>> {

	private final TinyORM orm;
	private final long entriesPerPage;
	private final Class<T> klass;
	private final Connection connection;

	PaginatedSelectStatement(Connection connection,
			Class<T> klass, TableMeta tableMeta, TinyORM orm, long entriesPerPage) {
		super(connection, tableMeta.getName());
		this.klass = klass;
		this.orm = orm;
		this.entriesPerPage = entriesPerPage;
		this.connection = connection;
	}

	public Paginated<T> execute() {
		Query query = this.limit(entriesPerPage + 1).buildQuery();
		try {
			try (PreparedStatement preparedStatement = this.connection
					.prepareStatement(query.getSQL())) {
				TinyORMUtil.fillPreparedStatementParams(preparedStatement,
						query.getValues());
				try (ResultSet rs = preparedStatement.executeQuery()) {
					List<T> rows = orm.mapRowListFromResultSet(klass, rs);

					final Paginated<T> paginated = new Paginated<T>(
							rows, entriesPerPage);
					return paginated;
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}

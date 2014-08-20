package me.geso.tinyorm;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PaginatedSelectStatement<T extends Row> extends
		AbstractSelectStatement<T, PaginatedSelectStatement<T>> {

	PaginatedSelectStatement(Connection connection, String tableName,
			Class<T> klass) {
		super(connection, tableName, klass);
	}

	public PaginatedWithCurrentPage<T> execute(long currentPage,
			long entriesPerPage) {
		Query query = this.limit(entriesPerPage + 1)
				.offset(entriesPerPage * (currentPage - 1)).buildQuery();
		try {
			ResultSet rs = TinyORM.prepare(connection, query.getSQL(),
					query.getValues()).executeQuery();
			List<T> rows = new ArrayList<>();
			while (rs.next()) {
				T row = TinyORM.mapResultSet(klass, rs, connection);
				rows.add(row);
			}

			boolean hasNextPage = false;
			if (rows.size() == entriesPerPage + 1) {
				rows.remove(rows.size() - 1); // pop tail
				hasNextPage = true;
			}

			final PaginatedWithCurrentPage<T> paginated = new PaginatedWithCurrentPage<T>(
					rows, currentPage, entriesPerPage, hasNextPage);
			return paginated;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}

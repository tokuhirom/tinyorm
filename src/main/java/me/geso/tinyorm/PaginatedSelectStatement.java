package me.geso.tinyorm;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;

public class PaginatedSelectStatement<T extends Row> extends
		AbstractSelectStatement<T, PaginatedSelectStatement<T>> {

	PaginatedSelectStatement(Connection connection, String tableName,
			Class<T> klass) {
		super(connection, tableName, klass);
	}

	public Paginated<T> execute(long currentPage, long entriesPerPage) {
		Query query = this.limit(entriesPerPage + 1)
				.offset(entriesPerPage * (currentPage - 1)).buildQuery();
		try {
			List<T> rows = new QueryRunner().query(connection, query.getSQL(),
					new BeanListHandler<T>(klass), query.getValues());
			boolean hasNextPage = false;
			if (rows.size() == entriesPerPage + 1) {
				rows.remove(rows.size() - 1); // pop tail
				hasNextPage = true;
			}

			final Paginated<T> paginated = new Paginated<T>(rows, currentPage, entriesPerPage, hasNextPage);
			return paginated;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}

package me.geso.tinyorm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import me.geso.tinyorm.meta.TableMeta;

public class PaginatedSelectStatement<T> extends
		AbstractSelectStatement<T, PaginatedSelectStatement<T>> {

	private final TableMeta tableMeta;

	PaginatedSelectStatement(Connection connection,
			Class<T> klass, TableMeta tableMeta) {
		super(connection, tableMeta.getName(), klass);
		this.tableMeta = tableMeta;
	}

	public PaginatedWithCurrentPage<T> execute(long currentPage,
			long entriesPerPage) {
		Query query = this.limit(entriesPerPage + 1)
				.offset(entriesPerPage * (currentPage - 1)).buildQuery();
		try {
			try (PreparedStatement preparedStatement = connection
					.prepareStatement(query.getSQL())) {
				TinyORMUtil.fillPreparedStatementParams(preparedStatement,
						query.getValues());
				try (ResultSet rs = preparedStatement.executeQuery()) {
					List<T> rows = new ArrayList<>();
					while (rs.next()) {
						T row = TinyORM.mapResultSet(klass, rs, connection,
								tableMeta);
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
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}

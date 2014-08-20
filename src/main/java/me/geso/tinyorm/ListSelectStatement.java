package me.geso.tinyorm;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ListSelectStatement<T extends Row> extends
		AbstractSelectStatement<T, ListSelectStatement<T>> {

	ListSelectStatement(Connection connection, String tableName, Class<T> klass) {
		super(connection, tableName, klass);
	}

	public List<T> execute() {
		Query query = this.buildQuery();
		try {
			ResultSet rs = TinyORM.prepare(connection, query.getSQL(),
					query.getValues()).executeQuery();
			List<T> rows = new ArrayList<>();
			while (rs.next()) {
				T row = TinyORM.mapResultSet(klass, rs, connection);
				rows.add(row);
			}
			return rows;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public Paginated<T> executeWithPagination(long entriesPerPage) {
		List<T> rows = this.limit(entriesPerPage + 1).execute();
		boolean hasNextPage = false;
		if (rows.size() == entriesPerPage + 1) {
			hasNextPage = true;
			rows.remove(rows.size() - 1);
		}
		return new Paginated<T>(rows, entriesPerPage, hasNextPage);
	}

}

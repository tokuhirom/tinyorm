package me.geso.tinyorm;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import me.geso.tinyorm.meta.TableMeta;

public class ListSelectStatement<T extends Row> extends
		AbstractSelectStatement<T, ListSelectStatement<T>> {

	private final TableMeta tableMeta;

	ListSelectStatement(Connection connection,
			Class<T> klass, TableMeta tableMeta) {
		super(connection, tableMeta.getName(), klass);
		this.tableMeta = tableMeta;
	}

	public List<T> execute() {
		Query query = this.buildQuery();
		try {
			ResultSet rs = TinyORM.prepare(connection, query.getSQL(),
					query.getValues()).executeQuery();
			List<T> rows = new ArrayList<>();
			while (rs.next()) {
				T row = TinyORM.mapResultSet(klass, rs, connection, tableMeta);
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

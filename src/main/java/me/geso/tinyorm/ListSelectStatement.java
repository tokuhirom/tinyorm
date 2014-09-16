package me.geso.tinyorm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ListSelectStatement<T> extends
		AbstractSelectStatement<T, ListSelectStatement<T>> {

	private final TableMeta tableMeta;
	private final TinyORM orm;
	private final Class<T> klass;

	ListSelectStatement(Connection connection,
			Class<T> klass, TableMeta tableMeta, TinyORM orm) {
		super(connection, tableMeta.getName());
		this.tableMeta = tableMeta;
		this.orm = orm;
		this.klass = klass;
	}

	public List<T> execute() {
		Query query = this.buildQuery();
		Connection connection = this.getConnection();
		try (PreparedStatement preparedStatement = connection
				.prepareStatement(query.getSQL())) {
			TinyORMUtil.fillPreparedStatementParams(preparedStatement,
					query.getValues());
			try (ResultSet rs = preparedStatement.executeQuery()) {
				List<T> rows = new ArrayList<>();
				while (rs.next()) {
					T row = orm.mapRowFromResultSet(klass, rs, tableMeta);
					rows.add(row);
				}
				return rows;
			}
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

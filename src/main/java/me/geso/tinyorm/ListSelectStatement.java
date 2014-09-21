package me.geso.tinyorm;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import me.geso.jdbcutils.JDBCUtils;
import me.geso.jdbcutils.Query;
import me.geso.jdbcutils.RichSQLException;

public class ListSelectStatement<T> extends
		AbstractSelectStatement<T, ListSelectStatement<T>> {

	private final TableMeta tableMeta;
	private final TinyORM orm;
	private final Class<T> klass;
	private final Connection connection;

	ListSelectStatement(Connection connection,
			Class<T> klass, TableMeta tableMeta, TinyORM orm) {
		super(connection, tableMeta.getName());
		this.tableMeta = tableMeta;
		this.orm = orm;
		this.klass = klass;
		this.connection = connection;
	}

	public List<T> execute() throws RichSQLException {
		Query query = this.buildQuery();
		return JDBCUtils.executeQuery(connection, query, (rs) -> {
			List<T> rows = new ArrayList<>();
			while (rs.next()) {
				T row = orm.mapRowFromResultSet(klass, rs, tableMeta);
				rows.add(row);
			}
			return rows;
		});
	}

	public Paginated<T> executeWithPagination(long entriesPerPage) throws RichSQLException {
		List<T> rows = this.limit(entriesPerPage + 1).execute();
		return new Paginated<T>(rows, entriesPerPage);
	}

}

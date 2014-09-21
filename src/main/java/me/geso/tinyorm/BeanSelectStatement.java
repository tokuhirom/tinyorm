package me.geso.tinyorm;

import java.sql.Connection;
import java.util.Optional;

import me.geso.jdbcutils.JDBCUtils;
import me.geso.jdbcutils.Query;
import me.geso.jdbcutils.RichSQLException;

public class BeanSelectStatement<T> extends
		AbstractSelectStatement<T, BeanSelectStatement<T>> {

	private final TableMeta tableMeta;
	private final TinyORM orm;
	private final Class<T> klass;
	private final Connection connection;

	BeanSelectStatement(Connection connection,
			Class<T> klass, TableMeta tableMeta, TinyORM orm) {
		super(connection, tableMeta.getName());
		this.tableMeta = tableMeta;
		this.orm = orm;
		this.klass = klass;
		this.connection = connection;
	}

	public Optional<T> execute() throws RichSQLException {
		Query query = this.buildQuery();

		return JDBCUtils.executeQuery(connection, query, (rs) -> {
				if (rs.next()) {
					T row = this.orm.mapRowFromResultSet(klass, rs, tableMeta);
					rs.close();
					return Optional.of(row);
				} else {
					return Optional.empty();
				}
		});
	}
}

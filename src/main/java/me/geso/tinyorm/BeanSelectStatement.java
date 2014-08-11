package me.geso.tinyorm;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;

public class BeanSelectStatement<T extends Row> extends AbstractSelectStatement<T, BeanSelectStatement<T>> {

	BeanSelectStatement(Connection connection, String tableName,
			Class<T> klass) {
		super(connection, tableName, klass);
	}

	public T execute() {
		Query query = this.buildQuery();
		try {
			return new QueryRunner().query(connection, query.getSQL(),
					new BeanHandler<T>(klass), query.getValues());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

}

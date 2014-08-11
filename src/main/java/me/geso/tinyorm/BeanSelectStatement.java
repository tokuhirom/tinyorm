package me.geso.tinyorm;

import java.util.Optional;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;

public class BeanSelectStatement<T extends Row> extends AbstractSelectStatement<T, BeanSelectStatement<T>> {

	BeanSelectStatement(Connection connection, String tableName,
			Class<T> klass) {
		super(connection, tableName, klass);
	}

	public Optional<T> execute() {
		Query query = this.buildQuery();
		try {
			T row = new QueryRunner().query(connection, query.getSQL(),
					new BeanHandler<T>(klass), query.getValues());
            return Optional.ofNullable(row);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

}

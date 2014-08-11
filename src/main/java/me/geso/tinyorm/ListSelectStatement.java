package me.geso.tinyorm;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;

public class ListSelectStatement<T extends Row> extends
		AbstractSelectStatement<T, ListSelectStatement<T>> {

	ListSelectStatement(Connection connection, String tableName, Class<T> klass) {
		super(connection, tableName, klass);
	}

	public List<T> execute() {
		Query query = this.buildQuery();
		try {
			return new QueryRunner().query(connection, query.getSQL(),
					new BeanListHandler<T>(klass), query.getValues());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

}

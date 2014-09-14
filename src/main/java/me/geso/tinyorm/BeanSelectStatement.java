package me.geso.tinyorm;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

public class BeanSelectStatement<T extends Row> extends AbstractSelectStatement<T, BeanSelectStatement<T>> {

	BeanSelectStatement(Connection connection, String tableName,
			Class<T> klass, BeanMapper orm) {
		super(connection, tableName, klass, orm);
	}

	public Optional<T> execute() {
		Query query = this.buildQuery();
		try {
			ResultSet rs = TinyORM.prepare(connection, query.getSQL(), query.getValues()).executeQuery();
			if (rs.next()) {
				T row = this.getBeanMapper().mapResultSet(klass, rs, connection);
				return Optional.of(row);
			} else {
				return Optional.empty();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

}

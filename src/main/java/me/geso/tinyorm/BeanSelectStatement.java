package me.geso.tinyorm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

public class BeanSelectStatement<T> extends
		AbstractSelectStatement<T, BeanSelectStatement<T>> {

	private final TableMeta tableMeta;
	private final TinyORM orm;

	BeanSelectStatement(Connection connection,
			Class<T> klass, TableMeta tableMeta, TinyORM orm) {
		super(connection, tableMeta.getName(), klass);
		this.tableMeta = tableMeta;
		this.orm = orm;
	}

	public Optional<T> execute() {
		Query query = this.buildQuery();
		try {
			String sql = query.getSQL();
			Object[] params= query.getValues();
			try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
				TinyORMUtil.fillPreparedStatementParams(preparedStatement, params);
				try (ResultSet rs = preparedStatement.executeQuery()) {
					if (rs.next()) {
						T row = this.orm.mapRowFromResultSet(klass, rs, tableMeta);
						rs.close();
						return Optional.of(row);
					} else {
						return Optional.empty();
					}
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}

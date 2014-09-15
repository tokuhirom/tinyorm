package me.geso.tinyorm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import me.geso.tinyorm.meta.TableMeta;

public class BeanSelectStatement<T extends Row> extends
		AbstractSelectStatement<T, BeanSelectStatement<T>> {

	private final TableMeta tableMeta;

	BeanSelectStatement(Connection connection,
			Class<T> klass, TableMeta tableMeta) {
		super(connection, tableMeta.getName(), klass);
		this.tableMeta = tableMeta;
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
						T row = TinyORM.mapResultSet(klass, rs, connection,
								tableMeta);
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

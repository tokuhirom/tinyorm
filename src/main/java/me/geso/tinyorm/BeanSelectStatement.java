package me.geso.tinyorm;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import me.geso.tinyorm.meta.TableMeta;

public class BeanSelectStatement<T extends Row> extends AbstractSelectStatement<T, BeanSelectStatement<T>> {


	private final TableMeta tableMeta;

	BeanSelectStatement(Connection connection, 
			Class<T> klass, TableMeta tableMeta) {
		super(connection, tableMeta.getName(), klass);
		this.tableMeta = tableMeta;
	}

	public Optional<T> execute() {
		Query query = this.buildQuery();
		try {
			ResultSet rs = TinyORM.prepare(connection, query.getSQL(), query.getValues()).executeQuery();
			if (rs.next()) {
				T row = TinyORM.mapResultSet(klass, rs, connection, tableMeta);
				return Optional.of(row);
			} else {
				return Optional.empty();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

}

package me.geso.tinyorm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import me.geso.jdbcutils.JDBCUtils;
import me.geso.jdbcutils.Query;
import me.geso.jdbcutils.UncheckedRichSQLException;

public class BeanSelectStatement<T extends Row<?>> extends
		AbstractSelectStatement<T, BeanSelectStatement<T>> {

	private final TableMeta<T> tableMeta;
	private final TinyORM orm;
	private final Class<T> klass;

	BeanSelectStatement(Connection connection,
			Class<T> klass, TableMeta<T> tableMeta, TinyORM orm) {
		super(connection, tableMeta.getName());
		this.tableMeta = tableMeta;
		this.orm = orm;
		this.klass = klass;
	}

	public Optional<T> execute() {
		Query query = this.buildQuery();

		final String sql = query.getSQL();
		final List<Object> params = query.getParameters();
		try (final PreparedStatement ps = this.orm.prepareStatement(sql)) {
			JDBCUtils.fillPreparedStatementParams(ps, params);
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final T row = this.tableMeta.createRowFromResultSet(
							this.klass,
							rs, this.orm);
					rs.close();
					return Optional.of(row);
				} else {
					return Optional.<T>empty();
				}
			}
		} catch (final SQLException ex) {
			throw new UncheckedRichSQLException(ex, sql, params);
		}
	}
}

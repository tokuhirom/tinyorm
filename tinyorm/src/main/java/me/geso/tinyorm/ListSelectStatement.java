package me.geso.tinyorm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import me.geso.jdbcutils.JDBCUtils;
import me.geso.jdbcutils.Query;
import me.geso.jdbcutils.UncheckedRichSQLException;

public class ListSelectStatement<T extends Row<?>> extends
		AbstractSelectStatement<T, ListSelectStatement<T>> {

	private final TableMeta<T> tableMeta;
	private final TinyORM orm;
	private final Class<T> klass;

	ListSelectStatement(Connection connection,
			Class<T> klass, TableMeta<T> tableMeta, TinyORM orm) {
		super(connection, tableMeta.getName());
		this.tableMeta = tableMeta;
		this.orm = orm;
		this.klass = klass;
	}

	public List<T> execute() {
		final Query query = this.buildQuery();

		final String sql = query.getSQL();
		final List<Object> params = query.getParameters();
		try (final PreparedStatement ps = isForUpdate() || isForceWriteConnection() ? orm.prepareStatement(sql) :
				orm.prepareStatementForRead(sql)) {
			JDBCUtils.fillPreparedStatementParams(ps, params);
			try (final ResultSet rs = ps.executeQuery()) {
				List<T> rows = new ArrayList<>();
				List<String> columnLabels = TinyORM.getColumnLabels(rs);
				while (rs.next()) {
					T row = tableMeta.createRowFromResultSet(klass, rs,
							columnLabels, this.orm);
					rows.add(row);
				}
				return rows;
			}
		} catch (final SQLException ex) {
			throw new UncheckedRichSQLException(ex, sql, params);
		}
	}

	public Paginated<T> executeWithPagination(long entriesPerPage) {
		final List<T> rows = this.limit(entriesPerPage + 1).execute();
		return new Paginated<>(rows, entriesPerPage);
	}

}

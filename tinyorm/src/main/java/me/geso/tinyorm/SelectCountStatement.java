package me.geso.tinyorm;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import me.geso.jdbcutils.JDBCUtils;
import me.geso.jdbcutils.Query;
import me.geso.jdbcutils.QueryBuilder;
import me.geso.jdbcutils.UncheckedRichSQLException;

/**
 * This class represents `SELECT COUNT(*)` statement.
 */
public class SelectCountStatement<T extends Row<?>> {
	private final String identifierQuoteString;
	private final String tableName;
	private final List<String> whereQuery = new ArrayList<>();
	private final List<Object> whereParams = new ArrayList<>();
	private final TinyORM orm;
	private boolean forceWriteConnection = false;

	SelectCountStatement(TableMeta<T> tableMeta, TinyORM orm) {
		this.tableName = tableMeta.getName();
		this.orm = orm;
		this.identifierQuoteString = orm.getIdentifierQuoteString();
	}

	public SelectCountStatement<T> where(String query, Object... params) {
		this.whereQuery.add(query);
		Collections.addAll(this.whereParams, params);
		return this;
	}

	/**
	 * Force to use write (master) connection for retrieval.
	 *
	 * Purpose: To counteract replication delays.
	 *
	 * If this method is not called, read-only connection will be used.
	 *
	 * @return Took over statement
	 */
	public SelectCountStatement<T> forceWriteConnection() {
		forceWriteConnection = true;
		return this;
	}

	public long execute() {
		final Query query = this.buildQuery();
		final String sql = query.getSQL();
		final List<Object> params = query.getParameters();
		try (final PreparedStatement ps = forceWriteConnection ? orm.prepareStatement(sql) :
				orm.prepareStatementForRead(sql)) {
			JDBCUtils.fillPreparedStatementParams(ps, params);
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					return rs.getLong(1);
				} else {
					return 0L;
				}
			}
		} catch (final SQLException ex) {
			throw new UncheckedRichSQLException(ex, sql, params);
		}
	}

	private Query buildQuery() {
		QueryBuilder builder = new QueryBuilder(this.identifierQuoteString)
			.appendQuery("SELECT COUNT(*) FROM ")
			.appendIdentifier(this.tableName);
		if (this.whereQuery != null && !this.whereQuery.isEmpty()) {
			builder.appendQuery(" WHERE ");
			builder.appendQuery(this.whereQuery.stream()
				.map(it -> "(" + it + ")")
				.collect(Collectors.joining(" AND ")));
			builder.addParameters(this.whereParams);
		}
		return builder.build();
	}
}

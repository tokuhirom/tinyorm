package me.geso.tinyorm;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import me.geso.jdbcutils.JDBCUtils;
import me.geso.jdbcutils.Query;
import me.geso.jdbcutils.QueryBuilder;
import me.geso.jdbcutils.RichSQLException;

/**
 * This class represents `SELECT COUNT(*)` statement.
 */
public class SelectCountStatement<T extends Row<?>> {
	private final Connection connection;
	private final String identifierQuoteString;
	private final String tableName;
	private final List<String> whereQuery = new ArrayList<>();
	private final List<Object> whereParams = new ArrayList<>();

	SelectCountStatement(TableMeta<T> tableMeta, Connection connection) {
		this.tableName = tableMeta.getName();
		this.connection = connection;
		try {
			this.identifierQuoteString = connection.getMetaData()
				.getIdentifierQuoteString();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public SelectCountStatement<T> where(String query, Object... params) {
		this.whereQuery.add(query);
		Collections.addAll(this.whereParams, params);
		return this;
	}

	public long execute() {
		try {
			final Query query = this.buildQuery();
			return JDBCUtils.executeQuery(
				this.connection,
				query,
				(rs) -> {
					if (rs.next()) {
						return rs.getLong(1);
					} else {
						return 0L;
					}
				}
				);
		} catch (RichSQLException e) {
			throw new RuntimeException(e);
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

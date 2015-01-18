package me.geso.tinyorm;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import me.geso.jdbcutils.Query;
import me.geso.jdbcutils.QueryBuilder;

public abstract class AbstractSelectStatement<T, Impl> {
	private final String tableName;
	private final String identifierQuoteString;
	private final List<String> whereQuery = new ArrayList<>();
	private final List<Object> whereParams = new ArrayList<>();
	private final List<String> orderBy = new ArrayList<>();
	private Long limit;
	private Long offset;
	private boolean forUpdate = false;

	AbstractSelectStatement(Connection connection, String tableName) {
		this.tableName = tableName;
		try {
			this.identifierQuoteString = connection.getMetaData()
					.getIdentifierQuoteString();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	public Impl where(String query, Object... params) {
		this.whereQuery.add(query);
		for (Object p : params) {
			this.whereParams.add(p);
		}
		return (Impl) this;
	}

	@SuppressWarnings("unchecked")
	public Impl limit(long limit) {
		this.limit = limit;
		return (Impl) this;
	}

	@SuppressWarnings("unchecked")
	public Impl offset(long offset) {
		this.offset = offset;
		return (Impl) this;
	}

	@SuppressWarnings("unchecked")
	public Impl orderBy(String orderBy) {
		this.orderBy.add(orderBy);
		return (Impl) this;
	}

	@SuppressWarnings("unchecked")
	public Impl orderBy(String key, Order order) {
		this.orderBy.add(new StringBuilder(key).append(" ")
				.append(order.toString()).toString());
		return (Impl) this;
	}

	@SuppressWarnings("unchecked")
	public Impl forUpdate() {
		this.forUpdate = true;
		return (Impl) this;
	}

	protected Query buildQuery() {
		QueryBuilder builder = new QueryBuilder(this.identifierQuoteString)
				.appendQuery("SELECT * FROM ")
				.appendIdentifier(this.tableName);
		if (this.whereQuery != null && !this.whereQuery.isEmpty()) {
			builder.appendQuery(" WHERE ");
			builder.appendQuery(this.whereQuery.stream()
					.map(it -> "(" + it + ")")
					.collect(Collectors.joining(" AND ")));
			builder.addParameters(this.whereParams);
		}
		if (!this.orderBy.isEmpty()) {
			builder.appendQuery(" ORDER BY ")
					.appendQuery(
							this.orderBy.stream().collect(Collectors.joining(",")));
		}
		if (this.limit != null) {
			builder.appendQuery(" LIMIT ")
					.appendQuery("" + this.limit);
		}
		if (this.offset != null) {
			builder.appendQuery(" OFFSET ")
					.appendQuery("" + this.offset);
		}
		if (this.forUpdate) {
			builder.appendQuery(" FOR UPDATE");
		}
		return builder.build();
	}
}

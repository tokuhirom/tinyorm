package me.geso.tinyorm;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class AbstractSelectStatement<T, Impl> {
	private final List<String> orderBy = new ArrayList<>();
	protected final Connection connection;
	private String tableName;
	protected Class<T> klass;
	private List<String> whereQuery = new ArrayList<>();
	private List<Object> whereParams = new ArrayList<>();
	private Long limit;
	private Long offset;

	AbstractSelectStatement(Connection connection, String tableName,
			Class<T> klass) {
		this.connection = connection;
		this.tableName = tableName;
		this.klass = klass;
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
		return (Impl)this;
	}

	@SuppressWarnings("unchecked")
	public Impl offset(long offset) {
		this.offset = offset;
		return (Impl)this;
	}

	@SuppressWarnings("unchecked")
	public Impl orderBy(String orderBy) {
		this.orderBy.add(orderBy);
		return (Impl) this;
	}

	protected Query buildQuery() {
		List<Object> params = new ArrayList<>();
		StringBuilder buf = new StringBuilder();
		buf.append("SELECT * FROM ").append(
				TinyORM.quoteIdentifier(tableName, connection));
		if (whereQuery != null && !whereQuery.isEmpty()) {
			buf.append(" WHERE ");
			buf.append(whereQuery.stream()
					.map(it -> "(" + it + ")")
					.collect(Collectors.joining(" AND ")));
			params.addAll(whereParams);
		}
		if (!orderBy.isEmpty()) {
			buf.append(" ORDER BY ");
			buf.append(orderBy.stream().collect(Collectors.joining(",")));
		}
		if (this.limit != null) {
			buf.append(" LIMIT ");
			buf.append(this.limit);
		}
		if (this.offset != null) {
			buf.append(" OFFSET ");
			buf.append(this.offset);
		}
		return new Query(buf.toString(), params);
	}
}

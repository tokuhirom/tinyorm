package me.geso.tinyorm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import me.geso.jdbcutils.JDBCUtils;
import me.geso.jdbcutils.Query;
import me.geso.jdbcutils.RichSQLException;

public class ListSelectStatement<T extends Row<?>> extends
		AbstractSelectStatement<T, ListSelectStatement<T>> {

	private final TableMeta<T> tableMeta;
	private final TinyORM orm;
	private final Class<T> klass;
	private final Connection connection;

	ListSelectStatement(Connection connection,
			Class<T> klass, TableMeta<T> tableMeta, TinyORM orm) {
		super(connection, tableMeta.getName());
		this.tableMeta = tableMeta;
		this.orm = orm;
		this.klass = klass;
		this.connection = connection;
	}

	/**
	 * Create stream from select statement.
	 * You must close the stream after use. I mean you should use try-with-resources for return value from this method.
	 *
	 * @return stream, that generates row objects.
	 */
	public Stream<T> executeStream() {
		final Query query = this.buildQuery();
		try {
			final PreparedStatement preparedStatement = connection.prepareStatement(query.getSQL());
			JDBCUtils.fillPreparedStatementParams(preparedStatement, query.getParameters());
			ResultSet resultSet = preparedStatement.executeQuery();
			ResultSetIterator<T> iterator = new ResultSetIterator<>(
				preparedStatement,
				resultSet,
				query.getSQL(),
				query.getParameters(),
				(rs) -> tableMeta.createRowFromResultSet(klass, rs,
					this.orm)
					);

			Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize(
				iterator, Spliterator.NONNULL | Spliterator.ORDERED | Spliterator.SIZED);
			final Stream<T> stream = StreamSupport.stream(spliterator, false);
			stream.onClose(() -> {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
				if (resultSet != null) {
					try {
						resultSet.close();
					} catch (SQLException e) {
						throw new RuntimeException(e);
					}
				}
			});
			return stream;
		} catch (SQLException e) {
			throw new RuntimeException(new RichSQLException(e, query.getSQL(), query.getParameters()));
		}
	}

	public List<T> execute() {
		return this.executeStream()
			.collect(Collectors.toList());
	}

	public Paginated<T> executeWithPagination(long entriesPerPage) {
		final List<T> rows = this.limit(entriesPerPage + 1).execute();
		return new Paginated<>(rows, entriesPerPage);
	}

}

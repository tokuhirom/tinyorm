package me.geso.tinyorm;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import me.geso.jdbcutils.RichSQLException;

public class ResultSetIterator<T> implements AutoCloseable, Iterator<T> {
	private final PreparedStatement preparedStatement;
	private final ResultSet resultSet;
	private final String query;
	private final List<Object> params;
	private final ResultSetIteratorCallback<T> callback;

	public ResultSetIterator(PreparedStatement preparedStatement, ResultSet resultSet, String query,
			List<Object> params, ResultSetIteratorCallback<T> callback) {
		this.preparedStatement = preparedStatement;
		this.resultSet = resultSet;
		this.query = query;
		this.params = params;
		this.callback = callback;
	}

	@Override
	public void close() throws Exception {
		if (preparedStatement != null) {
			preparedStatement.close();
		}
		if (resultSet != null) {
			resultSet.close();
		}
	}

	@Override
	public boolean hasNext() {
		try {
			return !this.resultSet.isLast();
		} catch (SQLException e) {
			throw new RuntimeException(new RichSQLException(e, query, params));
		}
	}

	@Override
	public T next() {
		try {
			this.resultSet.next();
			return callback.apply(this.resultSet);
		} catch (SQLException e) {
			throw new RuntimeException(new RichSQLException(e, query, params));
		}
	}
}

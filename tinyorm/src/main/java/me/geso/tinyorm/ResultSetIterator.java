package me.geso.tinyorm;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import me.geso.jdbcutils.RichSQLException;

public class ResultSetIterator<T> implements AutoCloseable, Iterator<T> {
    private final PreparedStatement preparedStatement;
    private final ResultSet resultSet;
    private final String query;
    private final List<Object> params;
    private final ResultSetIteratorCallback<T> callback;
    private boolean existsNext;

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
            this.existsNext = this.resultSet.next();
            return existsNext;
        } catch (SQLException e) {
            throw new RuntimeException(new RichSQLException(e, query, params));
        }
    }

    @Override
    public T next() {
        if (!this.existsNext) {
            throw new NoSuchElementException();
        }

        try {
            return callback.apply(this.resultSet);
        } catch (SQLException e) {
            throw new RuntimeException(new RichSQLException(e, query, params));
        }
    }

    public Stream<T> toStream() {
        Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize(
                this, Spliterator.NONNULL | Spliterator.ORDERED | Spliterator.SIZED);
        final Stream<T> stream = StreamSupport.stream(spliterator, false);
        stream.onClose(() -> {
            try {
                this.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return stream;
    }
}

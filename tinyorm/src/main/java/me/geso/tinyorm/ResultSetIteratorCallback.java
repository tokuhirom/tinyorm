package me.geso.tinyorm;

import java.sql.ResultSet;
import java.sql.SQLException;

@FunctionalInterface
public interface ResultSetIteratorCallback<T> {
	T apply(ResultSet resultSet) throws SQLException;
}

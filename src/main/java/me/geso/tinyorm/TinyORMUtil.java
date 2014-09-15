package me.geso.tinyorm;

import java.sql.PreparedStatement;
import java.sql.SQLException;

// Internal utility class
class TinyORMUtil {
	static void fillPreparedStatementParams(
			PreparedStatement preparedStatement,
			Object[] params) throws SQLException {
		for (int i = 0; i < params.length; ++i) {
			preparedStatement.setObject(i + 1, params[i]);
		}
	}

}

package me.geso.tinyorm;

import java.sql.Connection;
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

	/**
	 * Quote SQL identifier. You should get identifierQuoteString from
	 * DatabaseMetadata.
	 *
	 * @param identifier
	 * @param identifierQuoteString
	 * @return Escaped identifier.
	 */
	static String quoteIdentifier(String identifier,
			String identifierQuoteString) {
		return identifierQuoteString
				+ identifier.replace(identifierQuoteString,
						identifierQuoteString + identifierQuoteString)
				+ identifierQuoteString;
	}

	/**
	 * Quote SQL indentifier.
	 * 
	 * @param identifier
	 * @param connection
	 * @return
	 */
	static String quoteIdentifier(String identifier,
			Connection connection) {
		if (connection == null) {
			throw new NullPointerException();
		}
		try {
			String identifierQuoteString = connection.getMetaData()
					.getIdentifierQuoteString();
			return quoteIdentifier(identifier, identifierQuoteString);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

}

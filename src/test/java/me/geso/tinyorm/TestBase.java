package me.geso.tinyorm;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 *
 * @author Tokuhiro Matsuno <tokuhirom@gmail.com>
 */
public class TestBase {

	protected final Connection connection;
	protected final ORM orm;

	public TestBase() {
		try {
			// この指定で､ログとれる｡
			Class.forName("net.sf.log4jdbc.DriverSpy");
			Class.forName("com.mysql.jdbc.Driver").newInstance();

			connection = DriverManager.getConnection("jdbc:log4jdbc:mysql://localhost/test", "root", null);
			// connection = DriverManager.getConnection("jdbc:mysql://localhost/test?profileSQL=true&logger=com.mysql.jdbc.log.Slf4JLogger", "root", null);
			this.orm = new ORM();
			this.setupSchema();
		} catch (ClassNotFoundException | SQLException | InstantiationException | IllegalAccessException ex) {
			throw new RuntimeException(ex);
		}
	}

	public final void setupSchema() throws SQLException {
		connection.prepareStatement("DROP TABLE IF EXISTS member").executeUpdate();
		connection.prepareStatement("CREATE TABLE member (id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255))").executeUpdate();
	}

	public class ORM extends TinyORM {

		@Override
		public Connection getConnection() {
			return connection;
		}
	}

}

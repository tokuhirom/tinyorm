package me.geso.tinyorm;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.beanutils.BeanUtilsBean;

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

			String dburl = System.getProperty("test.dburl");
			String dbuser = System.getProperty("test.dbuser");
			String dbpassword = System.getProperty("test.dbpassword");
			if (dburl == null) {
				dburl = "jdbc:log4jdbc:mysql://localhost/test";
				dbuser = "root";
				dbpassword = "root";
			}

			connection = DriverManager.getConnection(dburl, dbuser, dbpassword);
			// connection =
			// DriverManager.getConnection("jdbc:mysql://localhost/test?profileSQL=true&logger=com.mysql.jdbc.log.Slf4JLogger",
			// "root", null);
			this.orm = new ORM();
			this.setupSchema();
		} catch (ClassNotFoundException | SQLException | InstantiationException
				| IllegalAccessException ex) {
			throw new RuntimeException(ex);
		}
	}

	public final void setupSchema() throws SQLException {
		connection.prepareStatement("DROP TABLE IF EXISTS member")
				.executeUpdate();
		connection
				.prepareStatement(
						"CREATE TABLE member (id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), createdOn INT UNSIGNED DEFAULT NULL, updatedOn INT UNSIGNED DEFAULT NULL)")
				.executeUpdate();
	}

	public class ORM extends TinyORM {
		@Override
		public Connection getConnection() {
			return connection;
		}

		@Override
		public <T extends Row> void BEFORE_INSERT(InsertStatement<T> insert) {
			try {
				BeanInfo beanInfo = Introspector.getBeanInfo(insert
						.getRowClass());
				PropertyDescriptor[] propertyDescriptors = beanInfo
						.getPropertyDescriptors();
				for (PropertyDescriptor prop : propertyDescriptors) {
					String name = prop.getName();
					if ("createdOn".equals(name)) {
						insert.value("createdOn",
								System.currentTimeMillis() / 1000);
					}
				}
			} catch (IntrospectionException e) {
				throw new RuntimeException(e);
			}
		}
	}

}

package me.geso.tinyorm;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
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
				dbpassword = "";
			}

			connection = DriverManager.getConnection(dburl, dbuser, dbpassword);
			// connection =
			// DriverManager.getConnection("jdbc:mysql://localhost/test?profileSQL=true&logger=com.mysql.jdbc.log.Slf4JLogger",
			// "root", null);
			this.orm = new ORM();
		} catch (ClassNotFoundException | SQLException | InstantiationException
				| IllegalAccessException ex) {
			throw new RuntimeException(ex);
		}
	}

	public class ORM extends TinyORM {
		@Override
		public Connection getConnection() {
			return connection;
		}
	}

	public static interface Callback {
		void run();
	}

	public void thrownLike(Callback code, String pattern) {
		RuntimeException gotEx = null;
		try {
			code.run();
		} catch (RuntimeException ex) {
			gotEx = ex;
			System.out.println(gotEx.toString());
			gotEx.printStackTrace();
		}
		assertNotNull(gotEx);
		String msg = gotEx.getMessage();
		if (msg.startsWith(pattern)) {
			log.error(msg);
		}
		assertTrue(msg.startsWith(pattern));
	}


}

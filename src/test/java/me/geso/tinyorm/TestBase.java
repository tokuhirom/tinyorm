package me.geso.tinyorm;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class TestBase {

	protected final Connection connection;
	protected final TinyORM orm;

	protected TestBase() {
		this.connection = buildConnection();
		this.orm = new TinyORM(connection);
	}

	protected Connection buildConnection() {
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

			return DriverManager.getConnection(dburl, dbuser, dbpassword);
		} catch (ClassNotFoundException | SQLException | InstantiationException
				| IllegalAccessException ex) {
			throw new RuntimeException(ex);
		}
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

	protected void createTable(String name, String... columns) {
		orm.updateBySQL("DROP TABLE IF EXISTS `" + name + "`");
		orm.updateBySQL("CREATE TABLE `" + name + "` ("
			+ Arrays.stream(columns).collect(Collectors.joining(","))
			+ ")");
	}

	public static interface Callback {
		void run();
	}

}

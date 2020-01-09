package me.geso.tinyorm.annotations;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import me.geso.jdbcutils.RichSQLException;
import me.geso.tinyorm.InsertStatement;
import me.geso.tinyorm.Row;
import me.geso.tinyorm.TestBase;
import org.junit.Test;
import org.slf4j.Logger;

import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

public class BeforeInsertTest extends TestBase {
	private static final Logger log = org.slf4j.LoggerFactory.getLogger(BeforeInsertTest.class);


	@Test
	public void test() throws SQLException, RichSQLException {
		this.createTable("x",
			"id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT",
			"name VARCHAR(255) NOT NULL",
			"y VARCHAR(255) NOT NULL");

		X created = orm.insert(X.class)
			.value("name", "John")
			.executeSelect();
		assertEquals("hoge", created.getY());
	}

	@Getter
	@Setter
	@ToString
	@Table("x")
	public static class X extends Row<X> {
		@PrimaryKey
		private long id;

		@Column
		private String name;

		@Column
		private String y;

		@BeforeInsert
		public static void beforeInsert(InsertStatement<X> stmt) {
			log.info("BEFORE INSERT");
			stmt.value("y", "hoge");
		}
	}
}

package me.geso.tinyorm.annotations;

import static org.junit.Assert.assertEquals;

import lombok.Getter;
import lombok.Setter;

import me.geso.jdbcutils.RichSQLException;
import me.geso.tinyorm.Row;
import me.geso.tinyorm.TestBase;

import org.junit.Test;

import java.sql.SQLException;

public class NamedColumnTest extends TestBase {
	@Test
	public void test() throws SQLException, RichSQLException {
		createTable("x",
			"id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT",
			"member_name VARCHAR(255) NOT NULL",
			"entry_name VARCHAR(255) NOT NULL");

		 X created = orm.insert(X.class)
			.value("member_name", "John")
			.value("entry_name", "Foo")
			.executeSelect();

		 assertEquals("John", created.getMemberName());
		 assertEquals("Foo", created.getEntryName());
	}

	@Getter
	@Setter
	@Table("x")
	public static class X extends Row<X> {
		@PrimaryKey
		private long id;

		@Column("member_name")
		private String memberName;

		@Column("entry_name")
		private String entryName;
	}
}

package me.geso.tinyorm.annotations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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

	@Test
	public void withUpdatedAndCreatedTimestampAnnotations() {
		createTable("y",
			"id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT",
			"created_at INT UNSIGNED NOT NULL",
			"updated_at INT UNSIGNED NOT NULL");

		Y created = orm.insert(Y.class)
			.executeSelect();

		assertNotNull(created.getCreatedAt());
		assertNotNull(created.getUpdatedAt());
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

	@Getter
	@Setter
	@Table("y")
	public static class Y extends Row<Y> {
		@PrimaryKey
		private long id;

		@Column("created_at")
		@CreatedTimestampColumn
		private long createdAt;

		@Column("updated_at")
		@UpdatedTimestampColumn
		private long updatedAt;
	}
}

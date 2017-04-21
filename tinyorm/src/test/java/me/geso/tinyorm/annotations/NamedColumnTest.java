package me.geso.tinyorm.annotations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.SQLException;
import java.util.Optional;

import org.junit.Test;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import me.geso.jdbcutils.RichSQLException;
import me.geso.tinyorm.Row;
import me.geso.tinyorm.TestBase;

public class NamedColumnTest extends TestBase {
	@Test
	public void test() throws SQLException, RichSQLException {
		createTable("x",
			"id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT",
			"member_name VARCHAR(255) NOT NULL",
			"entry_name VARCHAR(255) NOT NULL",
			"q_no VARCHAR(255) NOT NULL",
			"url VARCHAR(255) NOT NULL");

		X created = orm.insert(X.class)
			.value("member_name", "John")
			.value("entry_name", "Foo")
			.value("q_no", "qNo")
			.value("url", "http://example.com")
			.executeSelect();

		assertEquals("John", created.getMemberName());
		assertEquals("Foo", created.getEntryName());
		assertEquals("qNo", created.getQNo());
		assertEquals("http://example.com", created.getURL());
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

	@Test
	public void insertAndUpdateByBean() throws Exception {
		createTable("x",
					"id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT",
					"member_name VARCHAR(255) NOT NULL",
					"entry_name VARCHAR(255)",
					"q_no VARCHAR(255) NOT NULL",
					"url VARCHAR(255) NOT NULL");

		XForm form = new XForm();
		form.setMemberName("John");
		form.setQNo("qNo");
		form.setURL("http://example.com");

		X created = orm.insert(X.class)
					   .valueByBean(form)
					   .executeSelect();

		assertEquals("John", created.getMemberName());
		assertEquals(null, created.getEntryName());
		assertEquals("qNo", created.getQNo());
		assertEquals("http://example.com", created.getURL());

		XForm formForUpdate = new XForm();
		formForUpdate.setMemberName("Tom");
		formForUpdate.setQNo("P1");
		formForUpdate.setURL("http://other.com");

		created.update()
		   .setBean(formForUpdate)
		   .execute();

		Optional<X> updated = created.refetch();
		assertEquals("Tom", updated.get().getMemberName());
		assertEquals(null, updated.get().getEntryName());
		assertEquals("P1", updated.get().getQNo());
		assertEquals("http://other.com", updated.get().getURL());
	}

	@Data
	public static class XForm {
		private String memberName;
		private String qNo;
		private String URL;
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

		@Column("q_no")
		private String qNo;

		@Column("url")
		private String URL;
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

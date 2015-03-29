package me.geso.tinyorm;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

import org.junit.Before;
import org.junit.Test;

import lombok.EqualsAndHashCode;
import lombok.Value;
import me.geso.tinyorm.annotations.Column;
import me.geso.tinyorm.annotations.PrimaryKey;
import me.geso.tinyorm.annotations.Table;

public class UpdateRowStatementTest extends TestBase {
	@Before
	public void before() {
		createTable("member",
			"id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY",
			"name VARCHAR(255) NOt NULL");
	}

	@Test
	public void testHasSetClause() throws Exception {
		final Member member = orm.insert(Member.class)
			.value("name", "John")
			.executeSelect();
		assertFalse(member.update()
			.hasSetClause());
		assertTrue(member.update()
			.set("name", "Nick")
			.hasSetClause());
	}

	@EqualsAndHashCode(callSuper = false)
	@Value
	@Table("member")
	public static class Member extends Row<Member> {
		@PrimaryKey
		private long id;
		@Column
		private String name;
	}
}

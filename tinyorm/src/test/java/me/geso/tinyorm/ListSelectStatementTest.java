package me.geso.tinyorm;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import lombok.EqualsAndHashCode;
import lombok.Value;
import me.geso.tinyorm.annotations.Column;
import me.geso.tinyorm.annotations.PrimaryKey;
import me.geso.tinyorm.annotations.Table;

public class ListSelectStatementTest extends TestBase {
	@Before
	public void before() {
		createTable("member",
				"id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY",
				"name VARCHAR(255) NOT NULL");
	}

	@Test
	public void testExecuteStream() throws Exception {
		orm.insert(Member.class)
				.value("name", "John")
				.execute();
		orm.insert(Member.class)
				.value("name", "Nick")
				.execute();
		try (Stream<Member> stream = orm.search(Member.class)
				.orderBy("id DESC")
				.executeStream()) {
			assertThat(
					stream.map(Member::getName).collect(Collectors.joining(",")),
					is("Nick,John"));
		}
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

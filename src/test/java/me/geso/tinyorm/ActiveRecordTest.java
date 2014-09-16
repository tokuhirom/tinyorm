package me.geso.tinyorm;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.Optional;

import lombok.Data;
import lombok.EqualsAndHashCode;
import me.geso.tinyorm.annotations.Column;
import me.geso.tinyorm.annotations.CreatedTimestampColumn;
import me.geso.tinyorm.annotations.PrimaryKey;
import me.geso.tinyorm.annotations.Table;
import me.geso.tinyorm.annotations.UpdatedTimestampColumn;

import org.junit.Before;
import org.junit.Test;

public class ActiveRecordTest extends TestBase {
	@Before
	public final void setupSchema() throws SQLException {
		connection.prepareStatement("DROP TABLE IF EXISTS member")
				.executeUpdate();
		connection
				.prepareStatement(
						"CREATE TABLE member (id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), createdOn INT UNSIGNED DEFAULT NULL, updatedOn INT UNSIGNED DEFAULT NULL)")
				.executeUpdate();
	}

	@SuppressWarnings("unused")
	@Test
	public void testRefetch() {
		Member member1 = orm.insert(Member.class)
				.value("name", "John")
				.executeSelect();
		Member member2 = orm.insert(Member.class)
				.value("name", "Taro")
				.executeSelect();
		Member member3 = orm.insert(Member.class)
				.value("name", "Yuzo")
				.executeSelect();
		Optional<Member> got = member2.refetch();
		assertTrue(got.isPresent());
		assertEquals("Taro", got.get().getName());
	}

	@SuppressWarnings("unused")
	@Test
	public void testUpdateByBean() {
		Member member1 = orm.insert(Member.class)
				.value("name", "John")
				.executeSelect();
		Member member2 = orm.insert(Member.class)
				.value("name", "Taro")
				.executeSelect();
		Member member3 = orm.insert(Member.class)
				.value("name", "Yuzo")
				.executeSelect();
		MemberForm form = new MemberForm();
		form.setName("hoge");
		member2.updateByBean(form);
		Optional<Member> got = member2.refetch();
		assertTrue(got.isPresent());
		assertEquals("hoge", got.get().getName());
	}

	@SuppressWarnings("unused")
	@Test
	public void testCreateUpdateStatement() {
		Member member1 = orm.insert(Member.class)
				.value("name", "John")
				.executeSelect();
		Member member2 = orm.insert(Member.class)
				.value("name", "Taro")
				.executeSelect();
		Member member3 = orm.insert(Member.class)
				.value("name", "Yuzo")
				.executeSelect();
		member2.createUpdateStatement()
				.set("name", "hoge")
				.execute();
		Optional<Member> got = member2.refetch();
		assertTrue(got.isPresent());
		assertEquals("hoge", got.get().getName());
	}

	@SuppressWarnings("unused")
	@Test
	public void testDelete() {
		Member member1 = orm.insert(Member.class)
				.value("name", "John")
				.executeSelect();
		Member member2 = orm.insert(Member.class)
				.value("name", "Taro")
				.executeSelect();
		Member member3 = orm.insert(Member.class)
				.value("name", "Yuzo")
				.executeSelect();
		member2.delete();
		Optional<Member> got = member2.refetch();
		assertTrue(member1.refetch().isPresent());
		assertFalse(member2.refetch().isPresent());
		assertTrue(member3.refetch().isPresent());
	}

	@Table("member")
	@Data
	@EqualsAndHashCode(callSuper = false)
	public static class Member extends ActiveRecord<Member> {
		@PrimaryKey
		private long id;
		@Column
		private String name;

		@CreatedTimestampColumn
		private long createdOn;
		@UpdatedTimestampColumn
		private long updatedOn;
	}

	@Data
	public static class MemberForm {
		private String name;
	}
}

package me.geso.tinyorm;

import static org.junit.Assert.assertEquals;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.Data;
import lombok.EqualsAndHashCode;
import me.geso.tinyorm.annotations.Column;
import me.geso.tinyorm.annotations.CreatedTimestampColumn;
import me.geso.tinyorm.annotations.PrimaryKey;
import me.geso.tinyorm.annotations.Table;
import me.geso.tinyorm.annotations.UpdatedTimestampColumn;

import org.junit.Before;
import org.junit.Test;

public class TinyORMTest extends TestBase {

	@Before
	public final void setupSchema() throws SQLException {
		connection.prepareStatement("DROP TABLE IF EXISTS member")
				.executeUpdate();
		connection
				.prepareStatement(
						"CREATE TABLE member (id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), createdOn INT UNSIGNED DEFAULT NULL, updatedOn INT UNSIGNED DEFAULT NULL)")
				.executeUpdate();
	}

	@Test
	public void singleSimple() throws SQLException {
		orm.getConnection()
				.prepareStatement(
						"INSERT INTO member (name, createdOn, updatedOn) VALUES ('m1',1410581698,1410581698)")
				.executeUpdate();

		Member got = orm.singleBySQL(Member.class,
				"SELECT * FROM member WHERE name=?", new Object[] { "m1" })
				.get();
		assertEquals(1, got.getId());
		assertEquals("m1", got.getName());
	}

	@Test
	public void testInsert() throws SQLException, InstantiationException,
			IllegalAccessException {
		Member member = orm.insert(Member.class)
				.value("name", "John")
				.executeSelect();
		assertEquals(member.getName(), "John");
		assertEquals(member.getId(), 1);
		// assertNotEquals(0, member.getCreatedOn());
	}

	@Test
	public void insertByBean() throws SQLException {
		MemberForm form = new MemberForm();
		form.setName("Nick");
		Member member = orm.insert(Member.class)
				.valueByBean(form)
				.executeSelect();
		assertEquals(member.getName(), "Nick");
		assertEquals(member.getId(), 1);
	}

	@SuppressWarnings("unused")
	@Test
	public void single() throws SQLException {
		Member member1 = orm.insert(Member.class).value("name", "m1")
				.executeSelect();
		Member member2 = orm.insert(Member.class).value("name", "m2")
				.executeSelect();
		Member member3 = orm.insert(Member.class).value("name", "m3")
				.executeSelect();

		Member got = orm.singleBySQL(Member.class,
				"SELECT * FROM member WHERE name=?", new Object[] { "m2" })
				.get();
		assertEquals(got.getId(), member2.getId());
		assertEquals(got.getName(), "m2");
	}

	@SuppressWarnings("unused")
	@Test
	public void singleWithStmt() throws SQLException {
		Member member1 = orm.insert(Member.class).value("name", "m1")
				.executeSelect();
		Member member2 = orm.insert(Member.class).value("name", "m2")
				.executeSelect();
		Member member3 = orm.insert(Member.class).value("name", "m3")
				.executeSelect();

		Member got = orm.single(Member.class)
				.where("name=?", "m2")
				.execute().get();
		assertEquals(got.getId(), member2.getId());
		assertEquals(got.getName(), "m2");
	}

	@Test
	public void searchWithPager() throws SQLException {
		IntStream.rangeClosed(1, 10).forEach(i -> {
			orm.insert(Member.class).value("name", "m" + i)
					.executeSelect();
		});

		{
			Paginated<Member> paginated = orm.searchWithPager(
					Member.class, 4)
					.offset(0)
					.execute();
			assertEquals(paginated.getRows().size(), 4);
			assertEquals(paginated.getEntriesPerPage(), 4);
			assertEquals(paginated.getHasNextPage(), true);
		}
		{
			Paginated<Member> paginated = orm.searchWithPager(
					Member.class, 4)
					.offset(4)
					.execute();
			assertEquals(paginated.getRows().size(), 4);
			assertEquals(paginated.getEntriesPerPage(), 4);
			assertEquals(paginated.getHasNextPage(), true);
		}
		{
			Paginated<Member> paginated = orm.searchWithPager(
					Member.class, 4)
					.offset(8)
					.execute();
			assertEquals(paginated.getRows().size(), 2);
			assertEquals(paginated.getEntriesPerPage(), 4);
			assertEquals(paginated.getHasNextPage(), false);
		}
		{
			Paginated<Member> paginated = orm.searchWithPager(
					Member.class, 4)
					.offset(12)
					.execute();
			assertEquals(paginated.getRows().size(), 0);
			assertEquals(paginated.getEntriesPerPage(), 4);
			assertEquals(paginated.getHasNextPage(), false);
		}
	}

	@Test
	public void testSearchBySQL() throws SQLException {
		IntStream.rangeClosed(1, 10).forEach(i -> {
			orm.insert(Member.class).value("name", "m" + i)
					.execute();
		});

		{
			List<Member> members = orm
					.searchBySQL(
							Member.class,
							"SELECT id, id+1 AS idPlusOne FROM member ORDER BY id DESC",
							new Object[] {});
			System.out.println(members);
			assertEquals(10, members.size());
			assertEquals("10,9,8,7,6,5,4,3,2,1", members.stream()
					.map(row -> "" + row.getId())
					.collect(Collectors.joining(",")));
			assertEquals("11,10,9,8,7,6,5,4,3,2", members.stream()
					.map(row -> "" + row.getExtraColumn("idPlusOne"))
					.collect(Collectors.joining(",")));
		}
	}

	@Test
	public void testSearchBySQLWithPager() throws SQLException {
		IntStream.rangeClosed(1, 10).forEach(i -> {
			orm.insert(Member.class).value("name", "m" + i)
					.executeSelect();
		});

		{
			Paginated<Member> paginated = orm.searchBySQLWithPager(
					Member.class, "SELECT * FROM member ORDER BY id DESC",
					new Object[] {}, 4);
			assertEquals(paginated.getRows().size(), 4);
			assertEquals(paginated.getEntriesPerPage(), 4);
			assertEquals(paginated.getHasNextPage(), true);
			assertEquals("10,9,8,7", paginated.getRows().stream()
					.map(row -> "" + row.getId())
					.collect(Collectors.joining(",")));
		}
		{
			Paginated<Member> paginated = orm.searchBySQLWithPager(
					Member.class,
					"SELECT * FROM member WHERE id<7 ORDER BY id DESC",
					new Object[] {}, 4);
			assertEquals(paginated.getRows().size(), 4);
			assertEquals(paginated.getEntriesPerPage(), 4);
			assertEquals(paginated.getHasNextPage(), true);
			assertEquals("6,5,4,3", paginated.getRows().stream()
					.map(row -> "" + row.getId())
					.collect(Collectors.joining(",")));
		}
		{
			Paginated<Member> paginated = orm.searchBySQLWithPager(
					Member.class,
					"SELECT * FROM member WHERE id<? ORDER BY id DESC",
					new Object[] { 3 }, 4);
			assertEquals(paginated.getRows().size(), 2);
			assertEquals(paginated.getEntriesPerPage(), 4);
			assertEquals(paginated.getHasNextPage(), false);
			assertEquals("2,1", paginated.getRows().stream()
					.map(row -> "" + row.getId())
					.collect(Collectors.joining(",")));
		}
	}

	@SuppressWarnings("unused")
	@Test
	public void searchWithStmt() throws SQLException {
		Member member1 = orm.insert(Member.class).value("name", "m1")
				.executeSelect();
		Member member2 = orm.insert(Member.class).value("name", "m2")
				.executeSelect();
		Member member3 = orm.insert(Member.class).value("name", "b1")
				.executeSelect();

		List<Member> got = orm.search(Member.class)
				.where("name LIKE ?", "m%")
				.orderBy("id DESC")
				.execute();
		assertEquals(got.size(), 2);
		assertEquals(got.get(0).getName(), "m2");
		assertEquals(got.get(1).getName(), "m1");
	}

	@Test
	public void testQuoteIdentifier() throws SQLException {
		String got = TinyORM.quoteIdentifier("hoge", connection);
		assertEquals("`hoge`", got);
	}

	@Test
	public void testMapRowsFromResultSet() throws SQLException {
		orm.getConnection()
				.prepareStatement(
						"INSERT INTO member (name, createdOn, updatedOn) VALUES ('m1', UNIX_TIMESTAMP(NOW()), UNIX_TIMESTAMP(NOW()))")
				.executeUpdate();
		try (PreparedStatement ps = orm.getConnection().prepareStatement(
				"SELECT * FROM member")) {
			try (ResultSet rs = ps.executeQuery()) {
				orm.mapRowListFromResultSet(Member.class, rs);
			}
		}
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

		private long id;
		private String name;
	}

	@Data
	public static class MemberUpdateForm {
		private String name;

		public MemberUpdateForm(String name) {
			this.name = name;
		}

	}

}

package me.geso.tinyorm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.sql.SQLException;
import java.util.List;

import org.junit.Test;

/**
 *
 * @author Tokuhiro Matsuno <tokuhirom@gmail.com>
 */
public class TinyORMTest extends TestBase {

	public TinyORMTest() {
		super();
	}

	@Test
	public void insert() throws SQLException {
		Member member = orm.insert(Member.class).value("name", "John")
				.executeSelect();
		assertEquals(member.getName(), "John");
		assertEquals(member.getId(), 1);
		assertNotEquals(0, member.getCreatedOn());
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

		Member got = orm.single(Member.class,
				"SELECT * FROM member WHERE name=?", "m2").get();
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

	@SuppressWarnings("unused")
	@Test
	public void search() throws SQLException {
		Member member1 = orm.insert(Member.class).value("name", "m1")
				.executeSelect();
		Member member2 = orm.insert(Member.class).value("name", "m2")
				.executeSelect();
		Member member3 = orm.insert(Member.class).value("name", "b1")
				.executeSelect();

		List<Member> got = orm
				.search(Member.class,
						"SELECT * FROM member WHERE name LIKE ? ORDER BY id DESC",
						"m%");
		assertEquals(got.size(), 2);
		assertEquals(got.get(0).getName(), "m2");
		assertEquals(got.get(1).getName(), "m1");
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

}

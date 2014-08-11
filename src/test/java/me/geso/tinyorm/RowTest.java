package me.geso.tinyorm;

import java.sql.SQLException;
import java.util.Optional;

import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.junit.Assert;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 *
 * @author Tokuhiro Matsuno <tokuhirom@gmail.com>
 */
public class RowTest extends TestBase {


	static interface Callback {

		void run();
	}

	private void thrownLike(Callback code, String pattern) {
		RuntimeException gotEx = null;
		try {
			code.run();
		} catch (TinyORMException ex) {
			gotEx = ex;
			System.out.println(gotEx.toString());
		}
		Assert.assertNotNull(gotEx);
		String msg = gotEx.getMessage();
		assertTrue(msg.startsWith(pattern));
	}

	@Test
	public void testWhere() {
		thrownLike(() -> {
			Member member = new Member();
			member.setConnection(connection);
			Query where = member.where();
			System.out.println(where);
		}, "Primary key should not be zero");
	}

	@Test
	public void testRefetch() throws SQLException {
		Member member = orm.insert(Member.class).value("name", "John").executeSelect();
		Optional<Member> refetched = member.refetch();
		assertEquals(refetched.get().getName(), "John");
	}

	@Test
	public void testDelete() throws SQLException {
		Member taro = orm.insert(Member.class).value("name", "Taro").executeSelect();

		Member john = orm.insert(Member.class).value("name", "John").executeSelect();
		john.delete();
		long count = orm.query("SELECT COUNT(*) FROM member", new ScalarHandler<Long>(1));
		assertEquals(count, 1);
		assertTrue(taro.refetch().isPresent());
		assertFalse(john.refetch().isPresent());
	}

	@Test
	public void testUpdateByBean() {
		Member taro = orm.insert(Member.class).value("name", "Taro").executeSelect();
		Member member = orm.insert(Member.class).value("name", "John").executeSelect();
		MemberUpdateForm memberUpdateForm = new MemberUpdateForm("Nick");
		member.updateByBean(memberUpdateForm);
		assertEquals("Taro", taro.refetch().get().getName());
		assertEquals("Nick", member.refetch().get().getName());
	}
}

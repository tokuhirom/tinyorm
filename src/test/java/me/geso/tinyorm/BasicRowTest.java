package me.geso.tinyorm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.Optional;

import lombok.Data;
import lombok.EqualsAndHashCode;
import me.geso.tinyorm.annotations.BeforeInsert;
import me.geso.tinyorm.annotations.BeforeUpdate;
import me.geso.tinyorm.annotations.Column;
import me.geso.tinyorm.annotations.PrimaryKey;
import me.geso.tinyorm.annotations.Table;

import org.junit.Before;
import org.junit.Test;

public class BasicRowTest extends TestBase {

	@Before
	public void beforeeee() throws SQLException {
		orm.getConnection()
				.prepareStatement(
						"DROP TABLE IF EXISTS x")
				.executeUpdate();
		orm.getConnection()
				.prepareStatement(
						"CREATE TABLE x (id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255) NOT NULL, y VARCHAR(255) NOT NULL)")
				.executeUpdate();

	}

	@Test
	public void testCreateUpdateStatement() throws SQLException {
		X x = orm.insert(X.class)
				.value("name", "John")
				.executeSelect();
		assertEquals(x.getName(), "John");
		assertEquals(x.getId(), 1);
		assertEquals("inserted", x.getY());

		x.createUpdateStatement()
				.set("name", "Taro")
				.execute();
		;
		X refetched = x.refetch().get();
		assertEquals("Taro", refetched.getName());
		assertEquals(1, refetched.getId());
		assertEquals("updated", refetched.getY());
	}

	@Test
	public void testWhere() {
		thrownLike(() -> {
			X x = new X();
			x.setConnection(connection);
			x.setTableMeta(orm.getSchema().getTableMeta(X.class));
			Query where = x.where();
			System.out.println(where);
		}, "Primary key should not be zero");
	}

	@Test
	public void testRefetch() throws SQLException {
		X x = orm.insert(X.class)
				.value("name", "John")
				.executeSelect();
		Optional<X> refetched = x.refetch();
		assertEquals(refetched.get().getName(), "John");
	}

	@Test
	public void testDelete() throws SQLException {
		X taro = orm.insert(X.class).value("name", "Taro")
				.executeSelect();

		X john = orm.insert(X.class).value("name", "John")
				.executeSelect();
		john.delete();
		long count = orm.selectLong("SELECT COUNT(*) FROM x").getAsLong();
		assertEquals(1, count);
		assertTrue(taro.refetch().isPresent());
		assertFalse(john.refetch().isPresent());
	}

	@Test
	public void testUpdateByBean() {
		X taro = orm.insert(X.class)
				.value("name", "Taro")
				.executeSelect();
		X member = orm.insert(X.class)
				.value("name", "John")
				.executeSelect();
		XForm xform = new XForm();
		xform.setName("Nick");
		member.updateByBean(xform);
		assertEquals("Taro", taro.refetch().get().getName()); // not modified.
		assertEquals("Nick", member.refetch().get().getName());
	}

	@Data
	@Table("x")
	@EqualsAndHashCode(callSuper = false)
	public static class X extends BasicRow<X> {
		@PrimaryKey
		long id;
		@Column
		String name;
		@Column
		String y;

		@BeforeInsert
		public static void fillYBeforeInsert(InsertStatement<Row> stmt) {
			stmt.value("y", "inserted");
		}

		@BeforeUpdate
		public static void fillYBeforeUpdate(UpdateRowStatement stmt) {
			stmt.set("y", "updated");
		}
	}
	
	@Data
	public static class XForm {
		String name;
	}

}

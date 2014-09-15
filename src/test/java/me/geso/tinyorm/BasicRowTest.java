package me.geso.tinyorm;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.Optional;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import me.geso.tinyorm.annotations.BeforeInsert;
import me.geso.tinyorm.annotations.BeforeUpdate;
import me.geso.tinyorm.annotations.Column;
import me.geso.tinyorm.annotations.PrimaryKey;
import me.geso.tinyorm.annotations.Table;

import org.junit.Test;

public class BasicRowTest extends TestBase {

	@Test
	public void testCreateUpdateStatement() throws SQLException {
		orm.getConnection()
				.prepareStatement(
						"DROP TABLE IF EXISTS x")
				.executeUpdate();
		orm.getConnection()
				.prepareStatement(
						"CREATE TABLE x (id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255) NOT NULL, y VARCHAR(255) NOT NULL)")
				.executeUpdate();

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
	
	@Data
	@Table("x")
	@EqualsAndHashCode(callSuper=false)
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

}

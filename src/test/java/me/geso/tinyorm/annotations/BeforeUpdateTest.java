package me.geso.tinyorm.annotations;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import me.geso.tinyorm.ActiveRecord;
import me.geso.tinyorm.TestBase;
import me.geso.tinyorm.UpdateRowStatement;

import org.junit.Test;

public class BeforeUpdateTest extends TestBase {

	@Test
	public void test() throws SQLException {
		orm.getConnection()
				.prepareStatement(
						"DROP TABLE IF EXISTS x")
				.executeUpdate();
		orm.getConnection()
				.prepareStatement(
						"CREATE TABLE x (id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255) NOT NULL, y VARCHAR(255) NOT NULL)")
				.executeUpdate();
		X created = orm.insert(X.class)
				.value("name", "John")
				.value("y", "XXX")
				.executeSelect();
		XForm xform = new XForm();
		xform.setName("Ben");
		created.update()
				.setBean(xform)
				.execute();
		created = created.refetch().get();
		assertEquals("fuga", created.getY());
	}

	@Data
	public static final class XForm {

		private String name;

	}

	@Slf4j
	@Getter
	@Setter
	@Table("x")
	public static class X extends ActiveRecord<X> {
		@PrimaryKey
		private long id;

		@Column
		private String name;

		@Column
		private String y;

		@BeforeUpdate
		public static void beforeInsert(UpdateRowStatement stmt) {
			log.info("BEFORE UPDATE");
			stmt.set("y", "fuga");
		}
	}

}

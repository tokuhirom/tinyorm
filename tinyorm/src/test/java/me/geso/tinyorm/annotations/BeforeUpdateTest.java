package me.geso.tinyorm.annotations;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import me.geso.jdbcutils.RichSQLException;
import me.geso.tinyorm.Row;
import me.geso.tinyorm.TestBase;
import me.geso.tinyorm.UpdateRowStatement;
import org.junit.Test;
import org.slf4j.Logger;

import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

public class BeforeUpdateTest extends TestBase {
	private static final Logger log = org.slf4j.LoggerFactory.getLogger(BeforeUpdateTest.class);

	@Test
	public void test() throws SQLException, RichSQLException {
		createTable("x",
			"id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT",
			"name VARCHAR(255) NOT NULL",
			"y VARCHAR(255) NOT NULL");

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

	@Getter
	@Setter
	@Table("x")
	public static class X extends Row<X> {
		@PrimaryKey
		private long id;

		@Column
		private String name;

		@Column
		private String y;

		@BeforeUpdate
		public static void beforeInsert(UpdateRowStatement<X> stmt) {
			log.info("BEFORE UPDATE");
			stmt.set("y", "fuga");
		}
	}

}

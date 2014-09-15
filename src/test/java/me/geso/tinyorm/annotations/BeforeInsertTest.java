package me.geso.tinyorm.annotations;

import static org.junit.Assert.*;

import java.sql.SQLException;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import me.geso.tinyorm.BasicRow;
import me.geso.tinyorm.InsertStatement;
import me.geso.tinyorm.Row;
import me.geso.tinyorm.TestBase;

import org.junit.Test;

public class BeforeInsertTest extends TestBase {

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
		orm.getSchema().loadClass(X.class);
		X created = orm.insert(X.class)
				.value("name", "John")
				.executeSelect();
		assertEquals("hoge", created.getY());
	}

	@Slf4j
	@Data
	@Table("x")
	@EqualsAndHashCode(callSuper=false)
	public static class X extends BasicRow<X> {
		@PrimaryKey
		private long id;

		@Column
		private String name;

		@Column
		private String y;
		
		@BeforeInsert
		public static void beforeInsert(InsertStatement<Row> stmt) {
			log.info("BEFORE INSERT");
			stmt.value("y", "hoge");
		}
	}
}

package me.geso.tinyorm.feature;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import lombok.EqualsAndHashCode;
import lombok.Value;
import me.geso.jdbcutils.RichSQLException;
import me.geso.tinyorm.Row;
import me.geso.tinyorm.TestBase;
import me.geso.tinyorm.TinyORM;
import me.geso.tinyorm.annotations.Column;
import me.geso.tinyorm.annotations.PrimaryKey;
import me.geso.tinyorm.annotations.Table;

import org.junit.Test;

/**
 * Test a library state.
 */
public class LocalTimeTest extends TestBase {
	@Test
	public void test() throws SQLException, RichSQLException {
		this.createTable(
				"x",
				"id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY",
				"t TIME NOT NULL"
		);

		final LocalTime t = LocalTime.parse("12:34:56");
		final TinyORM tinyORM = new TinyORM(connection);
		final X x = tinyORM.insert(X.class)
				.value("t", t)
				.executeSelect();
		assertEquals("12:34:56", x.getT().format(DateTimeFormatter.ISO_LOCAL_TIME));
		assertEquals("12:34:56", x.refetch().get().getT().format(DateTimeFormatter.ISO_LOCAL_TIME));
	}

	@Test
	public void testNull() throws SQLException, RichSQLException {
		this.createTable(
				"x",
				"id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY",
				"t TIME DEFAULT NULL"
		);

		final TinyORM tinyORM = new TinyORM(connection);
		final X x = tinyORM.insert(X.class)
				.value("t", null)
				.executeSelect();
		assertNull(x.getT());
		assertNull(x.refetch().get().getT());
	}

	@Table("x")
	@Value
	@EqualsAndHashCode(callSuper = false)
	public static class X extends Row<X> {
		@PrimaryKey
		private long id;
		@Column
		private LocalTime t;
	}
}

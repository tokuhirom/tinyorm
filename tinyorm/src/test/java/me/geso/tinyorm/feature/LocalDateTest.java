package me.geso.tinyorm.feature;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.junit.Test;

import lombok.EqualsAndHashCode;
import lombok.Value;
import me.geso.jdbcutils.RichSQLException;
import me.geso.tinyorm.Row;
import me.geso.tinyorm.TestBase;
import me.geso.tinyorm.TinyORM;
import me.geso.tinyorm.annotations.Column;
import me.geso.tinyorm.annotations.PrimaryKey;
import me.geso.tinyorm.annotations.Table;

/**
 * Use LocalDate for "DATE" column.
 */
public class LocalDateTest extends TestBase {
	@Test
	public void test() throws SQLException, RichSQLException {
		this.createTable(
			"x",
			"id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY",
			"dt DATE NOT NULL"
			);

		final LocalDate dt = LocalDate.parse("2015-01-01");
		final TinyORM tinyORM = new TinyORM(connection);
		final X x = tinyORM.insert(X.class)
			.value("dt", dt)
			.executeSelect();
		assertEquals("2015-01-01", x.getDt().format(DateTimeFormatter.ISO_LOCAL_DATE));
		assertEquals("2015-01-01", x.refetch().get().getDt().format(DateTimeFormatter.ISO_LOCAL_DATE));
	}

	@Test
	public void testNull() throws SQLException, RichSQLException {
		this.createTable(
				"x",
				"id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY",
				"dt DATE DEFAULT NULL"
		);

		final TinyORM tinyORM = new TinyORM(connection);
		final X x = tinyORM.insert(X.class)
				.value("dt", null)
				.executeSelect();
		assertNull(x.getDt());
		assertNull(x.refetch().get().getDt());
	}

	@Table("x")
	@Value
	@EqualsAndHashCode(callSuper = false)
	public static class X extends Row<X> {
		@PrimaryKey
		private long id;
		@Column
		private LocalDate dt;
	}
}

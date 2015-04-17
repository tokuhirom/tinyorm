package me.geso.tinyorm.feature;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import org.junit.Test;

import lombok.EqualsAndHashCode;
import lombok.Value;
import me.geso.jdbcutils.RichSQLException;
import me.geso.tinyorm.Row;
import me.geso.tinyorm.TestBase;
import me.geso.tinyorm.TinyORM;
import me.geso.tinyorm.annotations.PrimaryKey;
import me.geso.tinyorm.annotations.Table;

/**
 * Use LocalDate as primary key
 */
public class LocalDatePKTest extends TestBase {
	@Test
	public void test() throws SQLException, RichSQLException {
		this.createTable(
				"x",
				"dt DATE NOT NULL PRIMARY KEY"
		);

		final LocalDate dt = LocalDate.parse("2015-01-01");
		final TinyORM tinyORM = new TinyORM(connection);
		tinyORM.insert(X.class)
				.value("dt", dt)
				.execute();
		final Optional<X> selected =
				tinyORM.single(X.class)
				.where("dt=?", dt.format(DateTimeFormatter.ISO_LOCAL_DATE))
				.execute();
		assertThat(selected.isPresent(), is(true));

		// delete it.
		selected.get().delete();
		final Optional<X> removed =
				tinyORM.single(X.class)
						.where("dt=?", dt)
						.execute();
		assertThat(removed.isPresent(), is(false));
	}

	@Table("x")
	@Value
	@EqualsAndHashCode(callSuper = false)
	public static class X extends Row<X> {
		@PrimaryKey
		private LocalDate dt;
	}
}

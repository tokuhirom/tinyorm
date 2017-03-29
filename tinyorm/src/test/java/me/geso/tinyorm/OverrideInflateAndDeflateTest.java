package me.geso.tinyorm;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import org.junit.Before;
import org.junit.Test;

import lombok.Data;
import lombok.EqualsAndHashCode;
import me.geso.tinyorm.annotations.Column;
import me.geso.tinyorm.annotations.Deflate;
import me.geso.tinyorm.annotations.Inflate;
import me.geso.tinyorm.annotations.PrimaryKey;
import me.geso.tinyorm.annotations.Table;
import me.geso.tinyorm.deflate.LocalTimeDeflater;
import me.geso.tinyorm.inflate.LocalTimeInflater;

public class OverrideInflateAndDeflateTest extends TestBase {
	@Before
	public void before() throws SQLException {
		orm.updateBySQL("DROP TABLE IF EXISTS x");
		createTable("x",
			"id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY",
			"t TIME");
	}

	@Test
	public void testForLocalTimeType() throws SQLException {
		LocalTime t = LocalTime.parse("12:34:56");
		X x = orm.insert(X.class)
			.value("t", t)
			.executeSelect();
		assertEquals("12:34:56", x.getT().format(DateTimeFormatter.ISO_LOCAL_TIME));
	}

	@Test
	public void testForStringType() {
		String t = "12:34:56";
		X x = orm.insert(X.class)
			.value("t", t)
			.executeSelect();
		assertEquals("12:34:56", x.getT().format(DateTimeFormatter.ISO_LOCAL_TIME));
	}

	@Test
	public void testForDefaultValue() {
		X x = orm.insert(X.class)
			.value("t", null)
			.executeSelect();
		assertEquals("00:00:00", x.getT().format(DateTimeFormatter.ISO_LOCAL_TIME));
	}

	@Data
	@EqualsAndHashCode(callSuper = false)
	@Table("x")
	public static class X extends Row<X> {
		private static final LocalTimeInflater LOCAL_TIME_INFLATER = new LocalTimeInflater();
		private static final LocalTimeDeflater LOCAL_TIME_DEFLATER = new LocalTimeDeflater();

		@PrimaryKey
		long id;
		@Column
		LocalTime t;

		@Inflate("t")
		public static Object inflateT(Object value) {
			if (value == null) {
				return LocalTime.parse("00:00:00");
			} else {
				return LOCAL_TIME_INFLATER.inflate(value);
			}
		}

		@Deflate("t")
		public static Object deflateT(Object value) {
			if (value instanceof LocalTime) {
				return LOCAL_TIME_DEFLATER.deflate(value);
			} else if (value instanceof String) {
				return LOCAL_TIME_DEFLATER.deflate(LocalTime.parse((String) value));
			} else if (value == null) {
				return null;
			} else {
				throw new IllegalArgumentException("deflateMaybeTime supports LocalTime, String");
			}
		}
	}
}
